import json
import google.generativeai as genai
from datetime import datetime
from ..utils.config import Config
from ..utils.db_handler import DBHandler
from ..utils.logger import Logger

class LLMExtractor:
    def __init__(self):
        self.logger = Logger("crawler_extract.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()
        self.llm = self._setup_llm_api()
        
    def _setup_llm_api(self):
        if not Config.GEMINI_API_KEY:
            self.logger.error("GEMINI_API_KEY is not set.")
            return None
        genai.configure(api_key=Config.GEMINI_API_KEY)
        return genai.GenerativeModel('gemini-1.5-flash')

    def _generate_prompt(self, text):
        prompt = f"""
        다음 텍스트에서 요리명, 재료(이름과 수량), 레시피(단계별 지시)를 추출해주세요.
        출력 형식은 반드시 JSON이어야 합니다.
        JSON의 키는 다음과 같습니다: "dish_name", "ingredients", "recipe".
        "ingredients"는 "name"과 "quantity" 키를 가진 객체 배열입니다.
        "recipe"는 "step"과 "instruction" 키를 가진 객체 배열입니다.

        텍스트:
        "{text}"
        """
        return prompt

    def _extract_with_llm(self, text):
        if not self.llm or not text:
            return None
        
        prompt = self._generate_prompt(text)
        try:
            response = self.llm.generate_content(prompt, stream=False)
            
            # Extract JSON from the raw response text.
            # LLM might return text with markdown fences (```json ... ```)
            text_response = response.text.strip()
            if text_response.startswith('```json'):
                text_response = text_response.replace('```json', '', 1)
            if text_response.endswith('```'):
                text_response = text_response.rstrip('`').rstrip()
            
            extracted_data = json.loads(text_response)
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"LLM extraction failed: {e}")
            return None

    def run(self):
        self.db.connect()
        if not self.db.conn:
            return
        
        videos = self.db.get_video_data()
        self.logger.info(f"Found {len(videos)} videos to extract.")
        
        success_count = 0
        for video in videos:
            try:
                video_data = video['data']
                video_id = video_data['video_id']
                
                # Check if already extracted
                if "dish_name" in video_data or "ingredients" in video_data or "recipe" in video_data:
                    self.logger.debug(f"[{video_id}] Already extracted. Skipping.")
                    continue
                
                # Extract clean description and captions
                clean_desc = video_data.get("clean_description", "")
                clean_captions = video_data.get("clean_captions", "")

                # Check if both are empty
                if not clean_desc and not clean_captions:
                    self.logger.warning(f"[{video_id}] No clean text available for extraction. Skipping.")
                    continue

                # Combine description and captions for LLM processing
                source_text = ""
                source_field = ""
                if clean_desc and clean_captions:
                    source_text = f"**설명:** {clean_desc}\n\n**자막:** {clean_captions}"
                    source_field = "description_and_captions"
                elif clean_desc:
                    source_text = clean_desc
                    source_field = "description"
                elif clean_captions:
                    source_text = clean_captions
                    source_field = "captions"
                else:
                    # 이 부분은 앞에서 이미 처리되지만, 로직의 견고성을 위해 유지
                    continue
                
                extracted_info = self._extract_with_llm(source_text)
                
                if extracted_info:
                    video_data.update(extracted_info)
                    video_data["metadata"]["source"] = source_field
                    video_data["metadata"]["extracted_at"] = datetime.now(datetime.UTC).isoformat()
                    
                    if self.db.insert_or_update_video(video_id, video_data):
                        success_count += 1
                        self.logger.info(f"[{video_id}] Successfully extracted recipe.")
                else:
                    self.logger.error(f"[{video_id}] Failed to extract recipe from text.")

            except Exception as e:
                self.logger.error(f"[{video['video_id']}] An error occurred during extraction: {e}")
        
        self.db.close()
        self.logger.info(f"LLM extraction finished. {success_count} videos processed successfully.")

if __name__ == "__main__":
    extractor = LLMExtractor()
    extractor.run()