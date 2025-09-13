import re
from datetime import datetime, timezone
from utils.config import Config
from utils.db_handler import DBHandler
from utils.logger import Logger
from utils.text_utils import merge_short_sentences, remove_conversational_fillers

class TextCleaner:
    def __init__(self):
        self.logger = Logger("text_cleaner.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()

    def _clean_description_text(self, raw_text):
        if not raw_text:
            return ""
        
        try:
            text = raw_text.strip()
            
            text = re.sub(r'https?://[^\s]+', '', text)
            text = re.sub(r'www\.[^\s]+', '', text)
            
            text = re.sub(r'@\w+', '', text)
            text = re.sub(r'#\w+', '', text)
            
            text = re.sub(r'\S+@\S+', '', text)
            
            text = re.sub(r'\b[A-Za-z]+\b', '', text)
            
            text = re.sub(r'[=\-_]{3,}', '', text)
            text = re.sub(r'[★☆♡♥♪♫♬♩]{2,}', '', text)
            text = re.sub(r'[ㅋㅎㅠㅜㅇ]{3,}', '', text)
            
            ad_patterns = [
                r'구독\s*좋아요', r'알림\s*설정', r'댓글\s*남겨', 
                r'팔로우', r'인스타', r'블로그', r'카페', r'카카오톡'
            ]
            for pattern in ad_patterns:
                text = re.sub(pattern, '', text, flags=re.IGNORECASE)
            
            greeting_patterns = [
                r'^안녕하세요[^\n]*\n?', r'^여러분[^\n]*\n?',
                r'^오늘도[^\n]*\n?', r'^이번\s*영상[^\n]*\n?'
            ]
            for pattern in greeting_patterns:
                text = re.sub(pattern, '', text, flags=re.MULTILINE)
            
            text = re.sub(r'(요|죠|네|예|입니다|이에요|예요|고|서|에|를|을|이|가|은|는|의|로|으로|에서|까지|부터|처럼|같이|그리고|하지만|그래서|또|또한)$', '', text)
            
            text = re.sub(r'\n\s*\n+', '\n', text)
            text = re.sub(r'\s+', ' ', text)
            
            text = merge_short_sentences(text, min_length=15)
            
            if not text.strip():
                text = re.sub(r'https?://[^\s]+|www\.[^\s]+|@\w+|#\w+|\S+@\S+|[★☆♡♥♪♫♬♩]{2,}|[ㅋㅎㅠㅜㅇ]{3,}', '', raw_text)
                text = re.sub(r'\s+', ' ', text).strip()
            
            return text.strip()
            
        except Exception as e:
            self.logger.error(f"Failed to clean description: {e}")
            return raw_text if raw_text else ""

    def _clean_captions_text(self, raw_captions):
        """자막 텍스트를 LLM 분석에 적합하게 정제 (토큰 최적화)"""
        if not raw_captions:
            return ""
        
        try:
            text = raw_captions.strip()
            
            text = re.sub(r'\[.*?\]', '', text)
            text = re.sub(r'\(.*?\)', '', text)
            text = re.sub(r'♪+.*?♪+', '', text)
            text = re.sub(r'[♪♫♬♩]+', '', text)
            
            filler_patterns = [
                r'\b(아|어|음|으|그|뭐|이제|자|네|예|좋아요|그렇죠|입니다|이에요|예요|요|죠|진짜|와|맛있다|맛있어|갑자기|비싸|배고프|놀랬어|놀랐어|좋아|오케이)\b',
                r'\b(하하|헤헤|히히|호호|크크|킥킥|웃음|효과음)\b',
                r'\b(어떻게|뭔가|아무튼|그냥|막|되게|완전|엄청|너무|아마|어쩌면|혹시|뭐랄까|그런데|저기|여기|이쪽|저쪽)\b',
                r'\b(그리고|하지만|그래서|또|또한|자|이제|먼저|다음|그럼|그래요|그래)\b'
            ]
            for pattern in filler_patterns:
                text = re.sub(pattern, '', text, flags=re.IGNORECASE)
            
            text = re.sub(r'(\b[\w\s]+)\s+\1+', r'\1', text)
            
            intro_outro_patterns = [
                r'^.*?(안녕하세요|여러분|시작해.*?볼게요|오늘은).*?\n',
                r'.*?(구독|좋아요|댓글|알림설정|갑자기.*?배고프|다음.*?보.*?요|감사합니다|고맙습니다).*?$',
                r'.*?(인플루언서|맛집|냄새.*?좋다|배고프|와\s*와|진짜\s*놀랐).*?[.!?]\s*'
            ]
            for pattern in intro_outro_patterns:
                text = re.sub(pattern, '', text, flags=re.MULTILINE | re.IGNORECASE)
            
            irrelevant_patterns = [
                r'.*?(날씨|주말|평일|영상|촬영|편집|업로드).*?[.!?]\s*'
            ]
            for pattern in irrelevant_patterns:
                text = re.sub(pattern, '', text)
            
            text = re.sub(r'(요|죠|네|예|입니다|이에요|예요|고|서|에|를|을|이|가|은|는|의|로|으로|에서|까지|부터|처럼|같이|그리고|하지만|그래서|또|또한)$', '', text)
            
            text = re.sub(r'\s+', ' ', text)
            
            text = merge_short_sentences(text, min_length=20)
            
            if not text.strip():
                text = re.sub(r'\[.*?\]|\(.*?\)|♪+.*?♪+|[♪♫♬♩]+|[하호히히흐][하호히히흐]{1,}|[오아우으][오아우으]{1,}', '', raw_captions)
                text = re.sub(r'\s+', ' ', text).strip()
            
            self.logger.info(f"Cleaned captions length: {len(text)}")
            return text.strip()
            
        except Exception as e:
            self.logger.error(f"Failed to clean captions: {e}")
            return raw_captions if raw_captions else ""

    def run(self):
        """정제 작업 실행"""
        print("텍스트 정제 시작...")
        
        self.db.connect()
        if not self.db.conn:
            print("데이터베이스 연결 실패")
            return
        
        videos = self.db.get_uncleaned_videos()
        print(f"{len(videos)}개 비디오 정제 대상 발견")
        self.logger.info(f"Found {len(videos)} videos to clean")

        if not videos:
            print("정제할 데이터가 없습니다")
            self.db.close()
            return

        success_count = 0
        for i, video in enumerate(videos, 1):
            try:
                video_data = video['data']
                video_id = video['video_id']
                
                print(f"처리 중... ({i}/{len(videos)}) {video_id}")
                
                raw_desc = video_data.get("raw_description", "")
                raw_captions = video_data.get("raw_captions", "")
                
                clean_desc = self._clean_description_text(raw_desc)
                clean_captions = self._clean_captions_text(raw_captions)
                
                video_data["clean_description"] = clean_desc
                video_data["clean_captions"] = clean_captions
                
                if "metadata" not in video_data:
                    video_data["metadata"] = {}
                video_data["metadata"]["cleaned_at"] = datetime.now(timezone.utc).isoformat()
                
                if self.db.insert_or_update_video(video_id, video_data):
                    success_count += 1
                    self.logger.info(f"[{video_id}] Successfully cleaned")
                else:
                    self.logger.error(f"[{video_id}] Failed to save cleaned data")
                    
            except Exception as e:
                video_id = video.get('video_id', 'unknown')
                print(f"오류 발생: {video_id} - {e}")
                self.logger.error(f"[{video_id}] Failed to clean: {e}")

        self.db.close()
        print(f"정제 완료! {success_count}/{len(videos)}개 성공")
        self.logger.info(f"Cleaning finished. {success_count}/{len(videos)} videos processed successfully")
        
        return success_count

if __name__ == "__main__":
    cleaner = TextCleaner()
    cleaner.run()