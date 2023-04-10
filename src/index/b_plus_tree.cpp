#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  root_page_id_=INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  buffer_pool_manager_->UnpinPage(root_page_id_,true);
  KeyType key{};
  auto* leaf= FindLeafPage(key,true);
   while(leaf->GetPageId()!=INVALID_PAGE_ID){
     auto* leaf_node= reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(leaf->GetData());
    auto* page=buffer_pool_manager_->FetchPage(leaf_node->GetNextPageId());
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
    buffer_pool_manager_->DeletePage(leaf->GetPageId());
    leaf=page;
  }
  buffer_pool_manager_->DeletePage(root_page_id_);
  root_page_id_=INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  Page* leaf_page=FindLeafPage(key,false);
 auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(leaf_page->GetData());
// auto* leaf= FindLeafPage(key,false,NULL);
  if(leaf== nullptr)
    return false;

  bool res=false;
  ValueType temp;
  if(leaf->Lookup(key,temp,comparator_)){
    result.push_back(temp);
    res=true;
  }
  page_id_t page_id=leaf->GetPageId();
  buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id,false);

  buffer_pool_manager_->UnpinPage(page_id,false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  return InsertIntoLeaf(key,value,transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page* page=buffer_pool_manager_->NewPage(root_page_id_);
  vector<page_id_t> temp;
  while(root_page_id_<2){
    temp.push_back(root_page_id_);
    page=buffer_pool_manager_->NewPage(root_page_id_);

  }
  while(temp.size()!=0){
    buffer_pool_manager_->UnpinPage(temp.back(),false);
    buffer_pool_manager_->DeletePage(temp.back());
    temp.pop_back();
  }
  if(page==nullptr)
    throw runtime_error("out of memory");
  auto root=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());
  UpdateRootPageId(true);
  root->Init(root_page_id_,INVALID_PAGE_ID,leaf_max_size_);
  root->Insert(key,value,comparator_);
  buffer_pool_manager_->UnpinPage(root->GetPageId(),true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page= FindLeafPage(key,false);
  if(page==nullptr)
    return false;
  auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());
  ValueType temp;
  if(leaf->Lookup(key,temp,comparator_)){
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
    return false;
  }

  if(leaf->GetSize()<leaf_max_size_)
    leaf->Insert(key,value,comparator_);

  else{
    auto* new_leaf= Split<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>>(leaf);
    if(comparator_(key,new_leaf->KeyAt(0))<0)
      leaf->Insert(key,value,comparator_);
    else
      new_leaf->Insert(key,value,comparator_);

    if(comparator_(leaf->KeyAt(0),new_leaf->KeyAt(0))<0){
      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf->GetPageId());
    }
    else
      new_leaf->SetNextPageId(leaf->GetPageId());

    InsertIntoParent(leaf,new_leaf->KeyAt(0),new_leaf);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page* page=buffer_pool_manager_->NewPage(page_id);
  vector<page_id_t> temp;
  while(page_id<2){
    temp.push_back(page_id);
    page=buffer_pool_manager_->NewPage(page_id);

  }
  while(temp.size()!=0){
    buffer_pool_manager_->UnpinPage(temp.back(),false);
    buffer_pool_manager_->DeletePage(temp.back());
    temp.pop_back();
  }
  if(page== nullptr){
    throw runtime_error("out of memory");
    return nullptr;
  }

  auto* new_node=reinterpret_cast<N*>(page->GetData());
  if(node->IsLeafPage()){
    new_node->Init(page_id,node->GetParentPageId(),leaf_max_size_);
  }
  else
    new_node->Init(page_id,node->GetParentPageId(),internal_max_size_);
  node->MoveHalfTo(new_node,buffer_pool_manager_);

  buffer_pool_manager_->UnpinPage(page_id,false);
  return new_node;

}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->IsRootPage()){
    //page_id_t new_root_page_id;
    auto* new_page=buffer_pool_manager_->NewPage(root_page_id_);
    vector<page_id_t> temp;
    while(root_page_id_<2){
      temp.push_back(root_page_id_);
      new_page=buffer_pool_manager_->NewPage(root_page_id_);

    }
    while(temp.size()!=0){
      buffer_pool_manager_->UnpinPage(temp.back(),false);
      buffer_pool_manager_->DeletePage(temp.back());
      temp.pop_back();
    }

    auto* new_root=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(new_page->GetData());
    new_root->Init(root_page_id_,INVALID_PAGE_ID,internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    UpdateRootPageId(false);


    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(),true);
  }

  else {
    auto *new_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto *father_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t , KeyComparator> *>(new_page->GetData());

    if (internal_max_size_ > father_node->GetSize()) {
      father_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(father_node->GetPageId());
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    }
    else {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(page_id);


      vector<page_id_t> temp_;
      while(page_id<2){
        temp_.push_back(page_id);
        page=buffer_pool_manager_->NewPage(page_id);
        if (page == nullptr) {
          throw runtime_error("out of memory");
        }

      }
      while(temp_.size()!=0){
        buffer_pool_manager_->UnpinPage(temp_.back(),false);
        buffer_pool_manager_->DeletePage(temp_.back());
        temp_.pop_back();
      }
      auto *temp = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
      temp->Init(page_id,INVALID_PAGE_ID,internal_max_size_);
      temp->SetSize(father_node->GetSize());
      int i = 0, j = 0;
      for (; i < father_node->GetSize(); i++, j++) {
        if (father_node->ValueAt(i) == old_node->GetPageId()) {
          temp->SetKeyAt(j, key);
          temp->SetValueAt(j,new_node->GetPageId());
          j++;
        }
        if (i < father_node->GetSize() - 1) {
          temp->SetKeyAt(j, father_node->KeyAt(i + 1));
          temp->SetValueAt(j,father_node->ValueAt(i+1));
        }
      }

      auto temp2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(temp);
      father_node->SetSize(temp->GetSize() + 1);
      for (i = 0; i < temp->GetSize(); i++) {
        father_node->SetKeyAt(i + 1, temp->KeyAt(i));
        father_node->SetValueAt(i+1,temp->ValueAt(i));
      }
      if (comparator_(key, temp2->KeyAt(0)) < 0) {
        new_node->SetParentPageId(father_node->GetPageId());
      }
      else if (comparator_(key, temp2->KeyAt(0)) == 0)
        new_node->SetParentPageId(temp2->GetPageId());
      else {
        new_node->SetParentPageId(temp2->GetPageId());
        old_node->SetParentPageId(temp2->GetPageId());
      }


      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(temp->GetPageId(), false);
      buffer_pool_manager_->DeletePage(temp->GetPageId());

      InsertIntoParent(father_node, temp2->KeyAt(0), temp2);
    }
    buffer_pool_manager_->UnpinPage(father_node->GetPageId(), true);
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty())
    return;
  Page* page= FindLeafPage(key,false);
  auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());
  if(leaf!= nullptr) {
    int size = leaf->GetSize();
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size) {
      CoalesceOrRedistribute(leaf, transaction);
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {

  if(node->IsRootPage()){
    return AdjustRoot(node);
  }
  Page* page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(page->GetData());
  int index=parent->ValueIndex(node->GetPageId());
  parent->SetKeyAt(index,node->KeyAt(0));
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);

  if(node->IsLeafPage()){
    if(node->GetSize()>=node->GetMinSize())
      return false;
  }
  else{
    if(node->GetSize()>node->GetMinSize())
      return false;
  }


  page_id_t sibling_page_id;
  if(index==0)
    sibling_page_id=parent->ValueAt(index+1);
  else
    sibling_page_id=parent->ValueAt(index-1);

  page=buffer_pool_manager_->FetchPage(sibling_page_id);

  N* sibling=reinterpret_cast<N*>(page->GetData());
  if(sibling->GetSize()+node->GetSize()>node->GetMaxSize()){
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
    Redistribute<N>(sibling,node,index);
    return false;
  }

  if(index==0){
    Coalesce<N>(&node,&sibling,&parent,1+index,transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
    return false;
  }
  else{
    Coalesce<N>(&sibling,&node,&parent,index,transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {

  (*node)->MoveAllTo(*neighbor_node,(*parent)->KeyAt(index),buffer_pool_manager_);
  (*parent)->Remove(index);
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(),true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  return CoalesceOrRedistribute(*parent,transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if(index==0){
    Page* page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto* parent=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(page->GetData());
    KeyType key=parent->KeyAt(parent->ValueIndex(node->GetPageId()));
    neighbor_node->MoveFirstToEndOf(node,key,buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),false);
  }

  else {
    KeyType key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);
    neighbor_node->MoveLastToFrontOf(node,key,buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->IsLeafPage()){
    if(old_root_node->GetSize()==0){
      root_page_id_=INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }

  if(old_root_node->GetSize()==1){
    auto* root=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(old_root_node);
    root_page_id_=root->ValueAt(0);
    UpdateRootPageId(false);

    auto new_root=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(old_root_node);
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    return true;

  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key{};
  Page* page= FindLeafPage( key,true);
  auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());

  return INDEXITERATOR_TYPE(leaf,0,buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page* page= FindLeafPage( key,false);
  auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());

int index=0;
  if(leaf!= nullptr){
    index=leaf->KeyIndex(key,comparator_);
  }

  return INDEXITERATOR_TYPE(leaf,index,buffer_pool_manager_);

}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {

  Page* page=buffer_pool_manager_->FetchPage((root_page_id_));
  auto* node=reinterpret_cast<BPlusTreePage*>(page->GetData());
  int index=0;
  while(!node->IsLeafPage()){
    auto* temp=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(node);
    page_id_t child_id;
    index=temp->GetSize()-1;
    child_id=temp->ValueAt(index);
    Page* child=buffer_pool_manager_->FetchPage(child_id);
    node=reinterpret_cast<BPlusTreePage*>(child->GetData());
  }
  page=buffer_pool_manager_->FetchPage(node->GetPageId());
  auto* leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());

  return INDEXITERATOR_TYPE(leaf,index,buffer_pool_manager_);

}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if(IsEmpty())
    return nullptr;
  Page* page=buffer_pool_manager_->FetchPage((root_page_id_));
  auto* node=reinterpret_cast<BPlusTreePage*>(page->GetData());

  while(!node->IsLeafPage()){
    auto* temp=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t ,KeyComparator>*>(node);
    page_id_t child_id;
    if(leftMost){
      child_id=temp->ValueAt(0);
    }
    else{
      child_id=temp->Lookup(key,comparator_);
    }
    Page* child=buffer_pool_manager_->FetchPage(child_id);
    node=reinterpret_cast<BPlusTreePage*>(child->GetData());

    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    page=child;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
  return buffer_pool_manager_->FetchPage(node->GetPageId());

}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {

  Page* page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);

//  if(page== nullptr)
//    ASSERT(false,"all pages are pinned");
  auto* header_page=reinterpret_cast<IndexRootsPage*>(page->GetData());
  if(insert_record==0){
    header_page->Update(index_id_,root_page_id_);
  }
  else
    header_page->Insert(index_id_,root_page_id_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

//INDEX_TEMPLATE_ARGUMENTS
//BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *
//BPlusTree<KeyType, ValueType, KeyComparator>::
//    FindLeafPage(const KeyType &key, bool leftMost,  Transaction *transaction_)
//{
//
//  if (IsEmpty())
//  {
//    return nullptr;
//  }
//
//  auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
//
//  auto *node = reinterpret_cast<BPlusTreePage *>(parent->GetData());
//  while (!node->IsLeafPage())
//  {
//    auto internal =
//        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
//                                               KeyComparator> *>(node);
//    page_id_t parent_page_id = node->GetPageId(), child_page_id;
//    if (leftMost)
//    {
//      child_page_id = internal->ValueAt(0);
//    }
//    else
//    {
//      child_page_id = internal->Lookup(key, comparator_);
//    }
//
//    auto *child = buffer_pool_manager_->FetchPage(child_page_id);
//
//    node = reinterpret_cast<BPlusTreePage *>(child->GetData());
//    assert(node->GetParentPageId() == parent_page_id);
//
//
//    //parent->RUnlatch();
//    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
//    parent = child;
//
//  }
//  return reinterpret_cast<BPlusTreeLeafPage<KeyType,
//                                            ValueType, KeyComparator> *>(node);
//}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
