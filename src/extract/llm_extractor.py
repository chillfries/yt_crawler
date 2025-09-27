# src/extract/llm_extractor.py

import json
import google.generativeai as genai
import re
import asyncio # 비동기 처리를 위해 추가
from datetime import datetime, timezone
from utils.config import Config
from utils.db_handler import DBHandler
from utils.logger import Logger

class LLMExtractor:
    def __init__(self):
        self.logger = Logger("llm_extractor.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()
        self.llm = self._setup_llm_api()
        # LLM 동시 요청 수 제어 세마포어 추가 (속도 개선)
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT) 
        
    def _setup_llm_api(self):
        if not hasattr(Config, 'GEMINI_API_KEY') or not Config.GEMINI_API_KEY:
            self.logger.error("GEMINI_API_KEY is not set in config.")
            return None
        genai.configure(api_key=Config.GEMINI_API_KEY)
        # 모델을 'gemini-2.5-flash'로 변경
        return genai.GenerativeModel('gemini-2.5-flash') 

    def _generate_prompt(self, text):
        # 카테고리, 난이도, 요리 시간을 포함한 프롬프트로 업데이트
        prompt = f"""다음 텍스트에서 요리 레시피 정보를 JSON으로 추출하세요.

요구사항:
- dish_name: 요리명 (필수)
- category: **이 요리의 핵심이 되는 카테고리(요리명)를 추출하세요.**
  - **규칙 1**: '고추장', '간장', '매운', '백종원'과 같은 **부가 설명**이나, '돼지고기', '소고기'처럼 **핵심 재료 외의 추가 재료**는 카테고리 이름에서 **제외**해야 합니다.
  - **규칙 2**: 요리 방식(예: '볶음', '구이')만 추출하는 것이 아니라, **요리명 자체**를 추출해야 합니다.
  - **예시 1**: dish_name이 '매콤한 고추장 오징어볶음'이면 category는 '오징어볶음'입니다.
  - **예시 2**: dish_name이 '돼지고기 오징어 볶음'이면 category는 '오징어볶음'입니다.
- ingredients: [ {{"name":"재료명", "quantity":"수량"}} ] 형태 배열
- recipe: [ {{"step":번호, "instruction":"조리과정"}} ] 형태 배열
- difficulty: 요리 난이도 (예: "쉬움", "보통", "어려움"). 텍스트에 없다면 빈 문자열.
- cooking_time: 총 요리 시간 (예: "30분", "1시간 30분"). 텍스트에 없다면 빈 문자열.
- JSON 형식만 응답

텍스트:
{text}

JSON:"""
        return prompt

    def _validate_extracted_data(self, extracted_info):
        # 필수 필드에 난이도, 요리 시간, 카테고리 추가
        if not extracted_info:
            return False, "No data extracted"
        
        required_fields = ['dish_name', 'category', 'ingredients', 'recipe', 'difficulty', 'cooking_time']
        for field in required_fields:
            if field not in extracted_info:
                return False, f"Missing field: {field}"
        
        dish_name = extracted_info.get('dish_name', '').strip()
        if not dish_name or len(dish_name) < 2:
            return False, "Dish name too short or empty"

        category = extracted_info.get('category', '').strip()
        if not category or len(category) < 2:
             return False, "Category name too short or empty"
        
        # 재료 및 레시피 배열 검증
        if not isinstance(extracted_info.get('ingredients'), list) or len(extracted_info['ingredients']) < 1:
            return False, "Ingredients list invalid or empty"
        if not isinstance(extracted_info.get('recipe'), list) or len(extracted_info['recipe']) < 1:
            return False, "Recipe list invalid or empty"
            
        return True, "Valid"

    def _extract_with_llm(self, text):
        prompt = self._generate_prompt(text)
        try:
            # 이 메서드는 run 메서드에서 asyncio.to_thread로 감싸져 동기적으로 실행됨
            response = self.llm.generate_content(prompt)
            
            # JSON 파싱
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response.text, re.DOTALL)
            if json_match:
                json_string = json_match.group(1)
            else:
                json_string = response.text.strip()

            extracted_info = json.loads(json_string)
            return extracted_info, ""
        except json.JSONDecodeError as e:
            return None, f"JSON decoding failed: {e}. Raw response: {response.text[:500]}..."
        except Exception as e:
            return None, f"LLM extraction error: {e}"

    def _clean_extracted_data(self, video_data):
        # 기존 데이터를 덮어쓰지 않고 업데이트합니다.
        # 이 메서드는 추출된 정보가 추가된 video_data를 받아 정리하는 역할
        video_data['metadata']['extracted_at'] = datetime.now(timezone.utc).isoformat()
        return video_data

    # NEW: 비동기 처리를 위한 헬퍼 메서드
    async def _process_video_async(self, index, total_count, video):
        video_id = video.get('video_id', 'unknown')
        video_data = video.get('data', {})
        
        async with self.semaphore: # 동시성 제한 적용
            self.logger.info(f"처리 중... ({index}/{total_count}) {video_id}")
            
            try:
                # 1. 텍스트 준비 및 길이 제한 적용 (속도 최적화)
                clean_desc = video_data.get('clean_description', '')
                clean_captions = video_data.get('clean_captions', '')
                
                MAX_CHARS = Config.LLM_MAX_INPUT_CHARS
                
                # 자막이 너무 길면 잘라냄
                if len(clean_captions) > MAX_CHARS:
                    clean_captions = clean_captions[:MAX_CHARS] + " [이후 자막 생략됨]"

                # 설명과 자막을 합치고 전체 길이 제한
                source_text = (clean_desc + " " + clean_captions).strip()
                if len(source_text) > MAX_CHARS:
                    source_text = source_text[:MAX_CHARS] + " [전체 텍스트 길이 제한으로 생략됨]"

                if not source_text:
                    reason = "Source text empty after cleaning."
                    self.db.insert_skipped_video(video_id, reason, f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                    self.logger.warning(f"[{video_id}] Skipping: {reason}")
                    return 0, 1 # (success, failed)
                
                # 2. LLM 추출 (await asyncio.to_thread로 동기 함수를 비동기로 호출)
                extracted_info, llm_error = await asyncio.to_thread(self._extract_with_llm, source_text)

                # 3. 데이터 품질 검증
                is_valid, reason = self._validate_extracted_data(extracted_info)
                
                if not is_valid:
                    self.db.insert_skipped_video(video_id, f"Quality validation failed: {reason}", f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                    self.logger.warning(f"[{video_id}] Quality validation failed: {reason}")
                    print(f"스킵: {video_id} - {reason}")
                    return 0, 1

                # 4. 데이터 통합 및 DB 저장
                video_data.update(extracted_info)
                cleaned_data = self._clean_extracted_data(video_data)
                
                if self.db.insert_or_update_video(video_id, cleaned_data):
                    dish_name = extracted_info.get('dish_name', 'Unknown')
                    print(f"성공: {video_id} - {dish_name}")
                    self.logger.info(f"[{video_id}] Successfully extracted: {dish_name}")
                    return 1, 0
                else:
                    self.logger.error(f"[{video_id}] Failed to save extracted data")
                    return 0, 1

            except Exception as e:
                self.logger.error(f"[{video_id}] Extraction error: {e}")
                print(f"오류: {video_id} - {e}")
                try:
                    self.db.insert_skipped_video(video_id, f"Processing error: {str(e)}", f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                except:
                    pass
                return 0, 1

    # run 메서드를 async 함수로 변경
    async def run(self):
        self.db.connect()
        videos = self.db.get_cleaned_videos()
        total_count = len(videos)
        print(f"\n[3단계: LLM 추출 시작] 대상: {total_count}개 비디오...")

        tasks = []
        for i, video in enumerate(videos, 1):
            tasks.append(self._process_video_async(i, total_count, video))

        # 모든 작업을 병렬로 실행
        results = await asyncio.gather(*tasks)
        
        success_count = sum(res[0] for res in results)
        failed_count = sum(res[1] for res in results)

        self.db.close()
        print("=" * 50)
        print(f"추출 완료! 성공: {success_count}개, 실패: {failed_count}개 (총 {total_count}개)")
        self.logger.info(f"Extraction finished. Success: {success_count}, Failed: {failed_count} (Total: {total_count})")
        return success_count