//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { this->capacity = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mtx);
  if (buffer.empty()) {
    return false;
  }
  *frame_id = (buffer[buffer.size() - 1]);
  buffer.erase(buffer.begin() + buffer.size() - 1);
  //   printf("buffersize=%lu\n", buffer.size());
  mtx.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  //   mtx.lock();
  std::lock_guard<std::mutex> guard(mtx);
  std::vector<int>::iterator i = buffer.begin();
  for (; i != buffer.end(); i++) {
    if (*i == frame_id) {
      break;
    }
  }
  if (i != buffer.end()) {
    buffer.erase(i);
  }
  //   mtx.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //   mtx.lock();
  std::lock_guard<std::mutex> guard(mtx);
  int i = 0;
  for (; i < static_cast<int>(buffer.size()); i++) {
    if (buffer[i] == frame_id) {
      return;
    }
  }
  buffer.insert(buffer.begin(), frame_id);
  if (buffer.size() > capacity) {
    buffer.pop_back();
  }
  //   mtx.unlock();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(mtx);
  return buffer.size();
}

}  // namespace bustub
