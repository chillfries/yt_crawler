from datetime import datetime
from ..utils.config import Config
from ..utils.db_handler import DBHandler
from ..utils.text_utils import (
    remove_links_and_hashtags, 
    remove_redundant_spaces, 
    clean_subtitle_text,
    merge_short_sentences
)
from ..utils.logger import Logger

class TextCleaner:
    def __init__(self):
        self.logger = Logger("crawler_clean.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()

    def _clean_description_text(self, raw_text):
        """설명 텍스트를 정제합니다."""
        if not raw_text:
            return ""
        try:
            # 링크와 해시태그 제거
            cleaned_text = remove_links_and_hashtags(raw_text)
            # 중복 공백 제거
            cleaned_text = remove_redundant_spaces(cleaned_text)
            return cleaned_text
        except Exception as e:
            self.logger.error(f"Failed to clean description text: {e}")
            return ""

    def _clean_captions_text(self, raw_captions):
        """자막 텍스트를 정제합니다."""
        if not raw_captions:
            return ""
        try:
            # 자막 특화 정제
            cleaned_text = clean_subtitle_text(raw_captions)
            # 링크와 해시태그 제거 (자막에도 포함될 수 있음)
            cleaned_text = remove_links_and_hashtags(cleaned_text)
            # 중복 공백 제거
            cleaned_text = remove_redundant_spaces(cleaned_text)
            # 너무 짧은 문장들 병합
            cleaned_text = merge_short_sentences(cleaned_text)
            return cleaned_text
        except Exception as e:
            self.logger.error(f"Failed to clean captions text: {e}")
            return ""

    def _analyze_text_quality(self, text):
        """텍스트의 품질을 분석합니다."""
        if not text:
            return {
                'length': 0,
                'word_count': 0,
                'has_cooking_keywords': False,
                'quality_score': 0
            }
        
        # 요리 관련 키워드 체크
        cooking_keywords = [
            '레시피', '요리', '재료', '만들기', '조리', '음식', '식재료',
            '그램', '스푼', '컵', '개', '마리', '송이', '뿌리', '줄기',
            '썰기', '볶기', '끓이기', '굽기', '찌기', '튀기기', '무치기',
            '양념', '소금', '후추', '마늘', '양파', '당근', '감자'
        ]
        
        has_cooking_keywords = any(keyword in text for keyword in cooking_keywords)
        word_count = len(text.split())
        
        # 품질 점수 계산 (0-100)
        quality_score = 0
        if has_cooking_keywords:
            quality_score += 40
        if word_count > 50:
            quality_score += 30
        if word_count > 200:
            quality_score += 20
        if len(text) > 500:
            quality_score += 10
        
        return {
            'length': len(text),
            'word_count': word_count,
            'has_cooking_keywords': has_cooking_keywords,
            'quality_score': quality_score
        }

    def run(self):
        self.db.connect()
        if not self.db.conn:
            return

        videos = self.db.get_video_data()
        self.logger.info(f"Found {len(videos)} videos to clean.")
        
        success_count = 0
        for video in videos:
            try:
                video_data = video['data']
                video_id = video_data['video_id']
                
                # Check if already cleaned
                if "clean_description" in video_data and "clean_captions" in video_data:
                    self.logger.debug(f"[{video_id}] Already cleaned. Skipping.")
                    continue
                
                raw_desc = video_data.get("raw_description", "")
                raw_captions = video_data.get("raw_captions", "")
                
                # 텍스트 정제
                clean_desc = self._clean_description_text(raw_desc)
                clean_captions = self._clean_captions_text(raw_captions)
                
                # 품질 분석
                desc_quality = self._analyze_text_quality(clean_desc)
                captions_quality = self._analyze_text_quality(clean_captions)
                
                # 데이터 업데이트
                video_data["clean_description"] = clean_desc
                video_data["clean_captions"] = clean_captions
                
                # 메타데이터에 품질 정보 추가
                video_data["metadata"]["cleaned_at"] = datetime.now(datetime.UTC).isoformat()
                video_data["metadata"]["text_quality"] = {
                    "description": desc_quality,
                    "captions": captions_quality,
                    "preferred_source": "description" if desc_quality['quality_score'] > captions_quality['quality_score'] else "captions"
                }
                
                if self.db.insert_or_update_video(video_id, video_data):
                    success_count += 1
                    self.logger.info(f"[{video_id}] Successfully cleaned. Desc quality: {desc_quality['quality_score']}, Captions quality: {captions_quality['quality_score']}")

            except Exception as e:
                self.logger.error(f"[{video.get('video_id', 'unknown')}] Failed to clean data: {e}")

        self.db.close()
        self.logger.info(f"Text cleaning finished. {success_count} videos processed successfully.")

if __name__ == "__main__":
    cleaner = TextCleaner()
    cleaner.run()