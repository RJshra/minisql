#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
  max_num_pages=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list_.empty()) return false;
  *frame_id=lru_list_.front();
  lru_list_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::list<frame_id_t>::iterator it;
  it=std::find(lru_list_.begin(),lru_list_.end(),frame_id);
  if(lru_list_.size()!=0 && !(it==lru_list_.end()&&lru_list_.back()!=frame_id)){
    lru_list_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::list<frame_id_t>::iterator it;
  it=std::find(lru_list_.begin(),lru_list_.end(),frame_id);
  if(lru_list_.size()==0){
    lru_list_.push_back(frame_id);
  }
  else{
    if(it==lru_list_.end()&&lru_list_.back()!=frame_id){
      if(lru_list_.size()==max_num_pages) lru_list_.pop_front();
      lru_list_.push_back(frame_id);
    }
    else{
      lru_list_.erase(it);
      lru_list_.push_back(frame_id);
    }
  }
}

size_t LRUReplacer::Size() {
  return lru_list_.size();
}