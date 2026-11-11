/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/ob_ik_ft_parser.h"

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "data_plane/fts/ob_fts_struct.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_letter_processor.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_quantifier_processor.h"
#include "storage/fts/ik/ob_ik_surrogate_processor.h"

namespace oceanbase
{
namespace storage
{
int ObIKFTParser::init(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    coll_type_ = ObCollationType::CS_TYPE_INVALID;
    if (OB_ISNULL(param.cs_) || OB_ISNULL(param.cs_->name)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (CS_TYPE_INVALID == (coll_type_ = ObCharset::collation_type(param.cs_->name))) {
      ret = OB_INVALID_ARGUMENT;
    } else if (FALSE_IT(ik_smart_ = (param.ik_param_.mode_ == ObFTIKParam::Mode::SMART))) {
    } else if (OB_FAIL(init_dict(param))) {
    } else if (OB_FAIL(init_ctx(param))) {
    } else if (OB_FAIL(init_segmenter(param))) {
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObIKFTParser::get_next_token(const char *&word,
                                 int64_t &word_len,
                                 int64_t &char_cnt,
                                 int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  const char *output_word;
  int64_t len;
  int64_t offset;
  int64_t cnt;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool accept_token = false;
    while (OB_SUCC(ret) && !accept_token) {
      if (OB_FAIL(produce())) {
      } else if (OB_FAIL(ctx_->get_next_token(output_word, len, offset, cnt))) {
        if (OB_ITER_END == ret) {
          // ok, end this iter
        } else {
        }
      } else {
        bool is_stop = false;
        // if (!OB_ISNULL(dict_stop_)
        //     && OB_FAIL(dict_stop_->match(ObString(len, output_word + offset), is_stop))) {
        //   LOG_WARN("Failed to match stopwords", K(ret));
        // } else
        if (!is_stop) {
          word = output_word + offset;
          word_len = len;
          char_cnt = cnt;
          word_freq = 1;
          accept_token = true;
        } else {
        }
      }
    }
  }

  return ret;
}

int ObIKFTParser::produce()
{
  int ret = OB_SUCCESS;
  // Loop until end or has data to output
  while (OB_SUCC(ret) && ctx_->result_list().empty() && !ctx_->iter_end()) {
    if (OB_FAIL(process_next_batch())) {
      if (OB_ITER_END == ret) {
        // ok
      } else {
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIKFTParser::process_one_char(TokenizeContext &ctx,
                                   const char *ch,
                                   const uint8_t char_len,
                                   const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  // proces by char with all segmenters
  for (ObList<ObIIKProcessor *, ObIAllocator>::iterator iter = segmenters_.begin();
       OB_SUCC(ret) && iter != segmenters_.end();
       iter++) {
    if (OB_FAIL((*iter)->process(ctx))) {
    }
  }
  return ret;
}

int ObIKFTParser::process_next_batch()
{
  int ret = OB_SUCCESS;
  ctx_->reset_resource();

  // handle next segmenter
  bool do_seg = false;

  if (ctx_->iter_end()) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !do_seg && !ctx_->iter_end()) {
      const char *ch;
      uint8_t char_len = 0;
      ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
      if (OB_FAIL(ctx_->current_char_and_type(ch, char_len, type))) {
      } else if (OB_FAIL(process_one_char(*ctx_, ch, char_len, type))) {
      } else {
        // 1. check segmention
        if (ctx_->handle_size() > SEGMENT_LIMIT && type == ObFTCharUtil::CharType::USELESS) {
          do_seg = true;
        }

        // 2. move to next;
        if (OB_FAIL(ctx_->step_next())) {
          if (OB_ITER_END == ret) {
          } else {
          }
        }
      } // end of one batch
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      arb_.reuse();
      if (OB_FAIL(arb_.process(*ctx_))) {
      } else if (OB_FAIL(arb_.output_result(*ctx_))) {
      }
    } else {
      // Already logged.
    }
  }

  return ret;
}

namespace
{
struct IKParserCache
{
  IKParserCache() : alloc_(ObMemAttr("IKParserCache")), parser_(nullptr), in_use_(false) {}
  ~IKParserCache()
  {
    if (OB_NOT_NULL(parser_)) {
      parser_->~ObIKFTParser();
      alloc_.free(parser_);
      parser_ = nullptr;
    }
  }
  ObArenaAllocator alloc_;
  ObIKFTParser *parser_;
  bool in_use_;
};
thread_local IKParserCache IK_PARSER_CACHE;
} // namespace

int ObIKFTParserDesc::segment(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObIKFTParser *parser = nullptr;
  ObFTDictHub *hub = nullptr;
  iter = nullptr;
  if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObFTParseData::instance().get_dict_hub(hub))) {
  } else if (!IK_PARSER_CACHE.in_use_) {
    if (OB_ISNULL(IK_PARSER_CACHE.parser_)) {
      if (OB_ISNULL(parser = OB_NEWx(ObIKFTParser,
                                     &IK_PARSER_CACHE.alloc_,
                                     IK_PARSER_CACHE.alloc_,
                                     hub))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(parser->init(*param))) {
        OB_DELETEx(ObIKFTParser, &IK_PARSER_CACHE.alloc_, parser);
        parser = nullptr;
      } else {
        IK_PARSER_CACHE.parser_ = parser;
      }
    } else if (OB_FAIL(IK_PARSER_CACHE.parser_->reuse_init(*param))) {
    } else {
      parser = IK_PARSER_CACHE.parser_;
    }
    if (OB_SUCC(ret)) {
      IK_PARSER_CACHE.in_use_ = true;
      iter = parser;
    }
  }

  // Re-entrant tokenization falls back to a caller-owned parser.
  if (OB_SUCC(ret) && OB_ISNULL(iter)) {
    if (OB_ISNULL(parser = OB_NEWx(ObIKFTParser, param->allocator_, *(param->allocator_), hub))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(parser->init(*param))) {
    } else {
      iter = parser;
    }
    if (OB_FAIL(ret)) {
      OB_DELETEx(ObIKFTParser, param->allocator_, parser);
    }
  }

  return ret;
}

void ObIKFTParserDesc::free_token_iter(ObFTParserParam *param,
                                       ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter) && iter == IK_PARSER_CACHE.parser_) {
    IK_PARSER_CACHE.in_use_ = false;
    iter = nullptr;
  } else if (OB_NOT_NULL(iter)) {
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
    iter = nullptr;
  }
}


int ObIKFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

int ObIKFTParser::init_dict(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
  }

  ObFTDictDesc main_dict_desc("main_dict",
                              ObFTDictType::DICT_IK_MAIN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc quan_dict_desc("quan_dict",
                              ObFTDictType::DICT_IK_QUAN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc stopword_dict_desc("stopword",
                                  ObFTDictType::DICT_IK_STOP,
                                  ObCharsetType::CHARSET_UTF8MB4,
                                  ObCollationType::CS_TYPE_UTF8MB4_BIN);

  if (OB_FAIL(ret)) {
  } else if (should_read_newest_table()) {
    // clear dict cache, always false now
  } else if (OB_FAIL(hub_->get_shared_dict(main_dict_desc, dict_main_))) {
  } else if (OB_FAIL(hub_->get_shared_dict(quan_dict_desc, dict_quan_))) {
  } else if (OB_FAIL(hub_->get_shared_dict(stopword_dict_desc, dict_stop_))) {
  } else {
    is_shared_dict_ = true;
  }

  return ret;
}

int ObIKFTParser::init_ctx(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (coll_type_ == common::CS_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_ = OB_NEWx(TokenizeContext,
                                      &work_alloc_,
                                      coll_type_,
                                      work_alloc_,
                                      param.fulltext_,
                                      param.ft_length_,
                                      ik_smart_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ctx_->init())) {
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(TokenizeContext, &work_alloc_, ctx_);
  }
  return ret;
}

int ObIKFTParser::init_segmenter(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  // do have an order
  ObIKLetterProcessor *letter_seg = nullptr;
  ObIKQuantifierProcessor *cnqsg = nullptr;
  ObIKCJKProcessor *cjksg = nullptr;
  ObIKSurrogateProcessor *surrogate_seg = nullptr;
  if (OB_ISNULL(letter_seg = OB_NEWx(ObIKLetterProcessor, &work_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_quan_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cnqsg = OB_NEWx(ObIKQuantifierProcessor, &work_alloc_, *dict_quan_, work_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_main_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cjksg = OB_NEWx(ObIKCJKProcessor, &work_alloc_, *dict_main_, work_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(surrogate_seg = OB_NEWx(ObIKSurrogateProcessor, &work_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(segmenters_.push_back(letter_seg))) {
  } else if (FALSE_IT(letter_seg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cnqsg))) {
  } else if (FALSE_IT(cnqsg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cjksg))) {
  } else if (FALSE_IT(cjksg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(surrogate_seg))) {
  } else if (OB_FALSE_IT(surrogate_seg = nullptr)) {
  }
  // push back by order, quantifier is before cjk

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKLetterProcessor, &work_alloc_, letter_seg);
    OB_DELETEx(ObIKQuantifierProcessor, &work_alloc_, cnqsg);
    OB_DELETEx(ObIKCJKProcessor, &work_alloc_, cjksg);
    OB_DELETEx(ObIKSurrogateProcessor, &work_alloc_, surrogate_seg);
  }
  return ret;
}

void ObIKFTParser::destroy_work()
{
  if (!OB_ISNULL(ctx_)) {
    ctx_->~TokenizeContext();
    work_alloc_.free(ctx_);
    ctx_ = nullptr;
  }

  for (ObIIKProcessor *segmenter : segmenters_) {
    if (!OB_ISNULL(segmenter)) {
      segmenter->~ObIIKProcessor();
      work_alloc_.free(segmenter);
    }
  }
  segmenters_.clear();
  arb_.reuse();
  work_alloc_.reset();
}

void ObIKFTParser::reset()
{
  destroy_work();

  if (!is_shared_dict_) {
    if (!OB_ISNULL(dict_main_)) {
      dict_main_->~ObIFTDict();
      allocator_.free(dict_main_);
    }
    if (!OB_ISNULL(dict_quan_)) {
      dict_quan_->~ObIFTDict();
      allocator_.free(dict_quan_);
    }
    if (!OB_ISNULL(dict_stop_)) {
      dict_stop_->~ObIFTDict();
      allocator_.free(dict_stop_);
    }
  }
  dict_main_ = nullptr;
  dict_quan_ = nullptr;
  dict_stop_ = nullptr;
  is_shared_dict_ = false;

  is_inited_ = false;
}

bool ObIKFTParser::can_reuse(const ObFTParserParam &param) const
{
  bool reusable = false;
  if (is_inited_ && OB_NOT_NULL(param.cs_) && OB_NOT_NULL(param.cs_->name)) {
    const ObCollationType coll_type = ObCharset::collation_type(param.cs_->name);
    reusable = coll_type == coll_type_
        && ik_smart_ == (param.ik_param_.mode_ == ObFTIKParam::Mode::SMART);
  }
  return reusable;
}

int ObIKFTParser::reuse_init(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  if (!can_reuse(param)) {
    reset();
    if (OB_FAIL(init(param))) {
    }
  } else {
    destroy_work();
    if (OB_FAIL(init_ctx(param))) {
      reset();
    } else if (OB_FAIL(init_segmenter(param))) {
      reset();
    }
  }
  return ret;
}

bool ObIKFTParser::should_read_newest_table() const { return false; }

} //  namespace storage
} //  namespace oceanbase
