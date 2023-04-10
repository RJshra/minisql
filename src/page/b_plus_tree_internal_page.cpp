#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(1);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first=key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int i=0;
  for(;i<GetSize();i++){
    if(array_[i].second==value)
      break;
  }
  return i;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  ValueType val=array_[index].second;
  return val;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second=value;

}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {

//  int low=0,high=GetSize(),middle=0;
//  ValueType val=array_[0].second;
//  while(low<high) {
//    middle=(low+high)/2;
//   if(comparator(array_[middle].first,key)==0)
//      return array_[middle].second;
//   else if(comparator(array_[middle].first,key)<0)
//      low=middle+1;
//   else
//      high=middle;
//  }
//  return val;
if (comparator(key, array_[1].first) < 0)
{
  return array_[0].second;
}
else if (comparator(key, array_[GetSize() - 1].first) >= 0)
{
  return array_[GetSize() - 1].second;
}

// 二分查找
  int low = 1, high = GetSize() - 1, mid;
  while (low < high && low + 1 != high)
  {
    mid = low + (high - low) / 2;
    if (comparator(key, array_[mid].first) < 0)
    {
      high = mid;
    }
    else if (comparator(key, array_[mid].first) > 0)
    {
      low = mid;
    }
    else
    {
      return array_[mid].second;
    }
  }
  return array_[low].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second=old_value;
  array_[1]={new_key,new_value};
  IncreaseSize(1);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  for(int i=GetSize()-1;i>=0;i--){
    if(array_[i].second==old_value){
      array_[i+1].first=new_key;
      array_[i+1].second=new_value;
      IncreaseSize(1);
      break;
    }
    array_[i+1]=array_[i];
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int half=(GetSize()+1)/2;
  MappingType* temp=&array_[GetSize()-half];
  recipient->CopyHalfFrom(temp,half,buffer_pool_manager);
  recipient->IncreaseSize(-1);
//  for(int i=GetSize()-half;i<GetSize();i++){
//    Page* page=buffer_pool_manager->FetchPage(ValueAt(i));
//    BPlusTreePage* child=reinterpret_cast<BPlusTreePage *>(page->GetData());
//    child->SetParentPageId(recipient->GetPageId());
//    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
//  }
  IncreaseSize(-1*half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  MappingType* temp=items;
  for(int i=0;i<size;i++){
    array_[GetSize()+i-1]=*temp;
    temp++;
  }
  IncreaseSize(size);
  for(int i=0;i<size;i++){
    Page* page=buffer_pool_manager->FetchPage(items[i].second);
    BPlusTreePage* child=reinterpret_cast<BPlusTreePage*>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
  }
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  MappingType* temp=items;
  for(int i=0;i<size;i++){
    array_[GetSize()+i]=*temp;
    temp++;
  }
  IncreaseSize(size);
  for(int i=0;i<size;i++){
    Page* page=buffer_pool_manager->FetchPage(items[i].second);
    BPlusTreePage* child=reinterpret_cast<BPlusTreePage*>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for(int i=index;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  return array_[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  Page* page=buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent= reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());

  SetKeyAt(0,middle_key);
  buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
  recipient->CopyNFrom(array_,GetSize(),buffer_pool_manager);
//  for(int i=0;i<GetSize();i++){
//    Page* temp=buffer_pool_manager->FetchPage((ValueAt(i)));
//    BPlusTreeInternalPage* child= reinterpret_cast<BPlusTreeInternalPage*>(temp->GetData());
//    child->SetParentPageId(recipient->GetPageId());
//    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
//  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType temp;
  temp.first= array_[0].first;
  temp.second= ValueAt(0);

  Page* page=buffer_pool_manager->FetchPage(GetParentPageId());
  auto* parent=reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()),array_[1].first);
  buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
  recipient->CopyLastFrom(temp,buffer_pool_manager);
  array_[0].second=array_[1].second;
  Remove(1);


}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  //  int index=parent->ValueIndex(GetPageId());
//  KeyType key=parent->KeyAt(index+1);

  //parent->SetKeyAt(index+1,pair.first);
//  Page* page=buffer_pool_manager->FetchPage(GetParentPageId());
//  BPlusTreeInternalPage* parent= reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
//  int index=parent->ValueIndex(GetPageId());

//  array_[GetSize()].first=parent->KeyAt(index+1);
//  parent->SetKeyAt(index+1,pair.first);
  array_[GetSize()].first=pair.first;
  array_[GetSize()].second=pair.second;
  IncreaseSize(1);

  Page* child_page=buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage* child=reinterpret_cast<BPlusTreePage*>(child_page->GetData());
  child->SetParentPageId(GetPageId());

  buffer_pool_manager->UnpinPage(child->GetPageId(),true);
//  buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType temp=array_[GetSize()-1];
  temp.first=middle_key;
  page_id_t  child_id=temp.second;
  recipient->CopyFirstFrom(temp,buffer_pool_manager);

  Page* page=buffer_pool_manager->FetchPage((child_id));
  auto* child= reinterpret_cast<BPlusTreePage*>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(),true);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  Page* page=buffer_pool_manager->FetchPage(GetParentPageId());
  auto* parent= reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  int index=parent->ValueIndex(GetPageId());
  for(int i=GetSize()-1;i>=0;i--)
    array_[i+1]=array_[i];
  //InsertNodeAfter(array_[0].second,parent->KeyAt(index),array_[0].second);
  parent->SetKeyAt(index,pair.first);
  array_[0]=pair;
  IncreaseSize(1);
  buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;