#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {
  leaf=new BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>;
  leaf->Init(INVALID_PAGE_ID);
  index=0;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>* leaf_node,
              int index_,BufferPoolManager* bufferPoolManager){
  leaf=leaf_node;
  index=index_;
  buffer_pool_manager_=bufferPoolManager;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  //buffer_pool_manager_->FetchPage(leaf->GetPageId());
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(),false);

}

/** Return the key/value pair this iterator is currently pointing at. */
INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(leaf!= nullptr&&!(index==leaf->GetSize()&&leaf->GetNextPageId()==INVALID_PAGE_ID));
  return leaf->GetItem(index);
}

/** Move to the next key/value pair.*/
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index++;
  //find next leaf
  if(index==leaf->GetSize()&&leaf->GetNextPageId()!=INVALID_PAGE_ID){
    page_id_t next_page_id=leaf->GetNextPageId();
    Page* page=buffer_pool_manager_->FetchPage(next_page_id);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),false);

    auto* next_leaf=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(page->GetData());
    index=0;
    leaf=next_leaf;
  }
  return *this;
}

/** Return whether two iterators are equal */
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (itr.leaf->GetPageId()==leaf->GetPageId()&&index==itr.index);
}

/** Return whether two iterators are not equal. */
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return (itr.leaf->GetPageId()!=leaf->GetPageId()||index!=itr.index);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
