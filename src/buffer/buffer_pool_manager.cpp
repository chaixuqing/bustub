//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  std::lock_guard<std::mutex> guard(latch_);
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.count(page_id) > 0) {
    // printf("exist\n");
    frame_id_t frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->Pin(frame_id);
    return &page;
  }
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_.insert(std::make_pair(page_id, frame_id));
    Page &page = pages_[frame_id];
    page.is_dirty_ = false;
    page.pin_count_ = 1;
    page.page_id_ = page_id;
    disk_manager_->ReadPage(page_id, page.data_);
    // replacer_->Pin(frame_id);
    return &page;
  }
  frame_id_t frame_id = -1;
  bool exist = replacer_->Victim(&frame_id);
  if (exist) {
    Page &page = pages_[frame_id];
    page_table_.erase(page.page_id_);
    page_table_.insert(std::make_pair(page_id, frame_id));
    if (page.is_dirty_) {
      disk_manager_->WritePage(page.page_id_, page.data_);
    }
    page.is_dirty_ = false;
    page.pin_count_ = 1;
    page.page_id_ = page_id;
    disk_manager_->ReadPage(page_id, page.data_);
    // replacer_->Pin(frame_id);
    return &page;
  }
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }
  page.pin_count_--;
  if (page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  if (!page.is_dirty_ && is_dirty) {
    page.is_dirty_ = true;
  }
  // if (page_id == 0) {
  //   printf("replacer.size=%lu\n", replacer_->Size());
  // }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];

  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // for (auto i : page_table_) {
  //   printf("NewPageImpl page_id=%d\n", i.first);
  //   printf("pages buffer page_id=%d\n", pages_[i.second].page_id_);
  //   // printf("frame_id=%d\n", i.second);
  // }

  std::lock_guard<std::mutex> guard(latch_);

  // printf("---------------------------\n");
  // printf("page_id=%d\n", pageId);
  // printf("--------------------------\n");
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // printf("free_list\n");
  } else {
    bool exist = replacer_->Victim(&frame_id);
    // printf("victim\n");
    // printf("replacer_size=%lu\n", replacer_->Size());
    if (!exist) {
      return nullptr;
    }
  }
  Page &page = pages_[frame_id];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.page_id_, page.GetData());
  }
  page_id_t pageId = disk_manager_->AllocatePage();
  // printf("NewPageImpl victim_page_id=%d\n", page.page_id_);
  page_table_.erase(page.page_id_);

  page.pin_count_ = 1;
  page.is_dirty_ = false;
  page.page_id_ = pageId;
  page.ResetMemory();
  page_table_.insert(std::make_pair(pageId, frame_id));
  *page_id = pageId;
  // for (auto i : page_table_) {
  //   printf("page_id=%d\n", i.first);
  //   // printf("frame_id=%d\n", i.second);
  // }
  return &page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }
  replacer_->Pin(frame_id);
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.page_id_, page.GetData());
  }
  page.ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub
