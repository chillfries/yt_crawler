import json
import google.generativeai as genai
import re
from datetime import datetime, timezone
from utils.config import Config
from utils.db_handler import DBHandler
from utils.logger import Logger

class LLMExtractor:
    def __init__(self):
        self.logger = Logger("llm_extractor.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()
        self.llm = self._setup_llm_api()
        
    def _setup_llm_api(self):
        if not hasattr(Config, 'GEMINI_API_KEY') or not Config.GEMINI_API_KEY:
            self.logger.error("GEMINI_API_KEY is not set in config.")
            return None
        genai.configure(api_key=Config.GEMINI_API_KEY)
        return genai.GenerativeModel('gemini-2.5-flash')

    def _validate_extracted_data(self, extracted_info):
        if not extracted_info:
            return False, "No data extracted"
        
        required_fields = ['dish_name', 'category', 'ingredients', 'recipe', 'difficulty', 'cooking_time']
        for field in required_fields:
            if field not in extracted_info:
                return False, f"Missing field: {field}"
        
        dish_name = extracted_info.get('dish_name', '').strip()
        if not dish_name or len(dish_name) < 2:
            return False, "Dish name too short or empty"
        
        ingredients = extracted_info.get('ingredients', [])
        if not isinstance(ingredients, list) or len(ingredients) < 2:
            return False, f"Insufficient ingredients: {len(ingredients) if isinstance(ingredients, list) else 0}"
        
        valid_ingredients = 0
        for ing in ingredients:
            if isinstance(ing, dict) and ing.get('name', '').strip() and ing.get('quantity', '').strip():
                if len(ing['name'].strip()) >= 2:
                    valid_ingredients += 1
        
        if valid_ingredients < 2:
            return False, f"Too few valid ingredients: {valid_ingredients}"
        
        recipe = extracted_info.get('recipe', [])
        if not isinstance(recipe, list) or len(recipe) < 2:
            return False, f"Insufficient recipe steps: {len(recipe) if isinstance(recipe, list) else 0}"
        
        valid_steps = 0
        for step in recipe:
            if isinstance(step, dict) and step.get('instruction', '').strip():
                instruction = step['instruction'].strip()
                if len(instruction) >= 10:
                    valid_steps += 1
        
        if valid_steps < 2:
            return False, f"Too few valid recipe steps: {valid_steps}"
        
        difficulty = extracted_info.get('difficulty', '').strip()
        if difficulty:
            valid_difficulties = ['매우 쉬움', '쉬움', '보통', '어려움', '매우 어려움']
            if difficulty not in valid_difficulties:
                extracted_info['difficulty'] = '보통'
        else:
            extracted_info['difficulty'] = '보통'
        
        cooking_time = extracted_info.get('cooking_time', '').strip()
        if not cooking_time:
            extracted_info['cooking_time'] = '정보 없음'

        category = extracted_info.get('category', '').strip()
        if not category or len(category) < 2:
            return False, "Category name too short or empty"
            
        return True, "Valid"    

    def _generate_prompt(self, text):
        prompt = f"""다음 텍스트에서 요리 레시피 정보를 JSON으로 추출하세요.

요구사항:
- dish_name: 요리명 (필수)
- category: 이 요리의 핵심이 되는 카테고리(요리명)를 추출하세요.
  - 규칙 1: '고추장', '간장', '매운', '백종원'과 같은 **부가 설명**이나, '돼지고기', '소고기'처럼 **핵심 재료 외의 추가 재료**는 카테고리 이름에서 **제외**해야 합니다.
  - 규칙 2: 요리 방식(예: '볶음', '구이')만 추출하는 것이 아니라, **요리명 자체**를 추출해야 합니다.
  - 예시 1: dish_name이 '매콤한 고추장 오징어볶음'이면 category는 '오징어볶음'입니다.
  - 예시 2: dish_name이 '돼지고기 오징어 볶음'이면 category는 '오징어볶음'입니다.
  - 예시 3: dish_name이 '김치 돼지고기 짜글이'면 category는 '김치짜글이' 또는 '짜글이'입니다.
- ingredients: [{{"name":"재료명", "quantity":"수량"}}] 형태 배열  
- recipe: [{{"step":번호, "instruction":"조리과정"}}] 형태 배열
- difficulty: 요리 난이도 ("매우 쉬움", "쉬움", "보통", "어려움", "매우 어려움" 중 하나)
- cooking_time: 총 요리 시간 (예: "30분", "1시간 30분")
- JSON 형식만 응답

텍스트:
{text}

JSON:"""
        return prompt

    def _extract_with_llm(self, text):
        if not self.llm:
            return None

        prompt = self._generate_prompt(text)
        
        try:
            response = self.llm.generate_content(prompt)
            
            match = re.search(r'```json\n(.*?)```', response.text, re.DOTALL)
            if match:
                json_string = match.group(1).strip()
                result = json.loads(json_string)
                
                if all(key in result for key in ['dish_name', 'ingredients', 'recipe']):
                    return result
                else:
                    self.logger.warning("Missing required fields in LLM response")
                    return None

            try:
                result = json.loads(response.text.strip())
                if all(key in result for key in ['dish_name', 'ingredients', 'recipe']):
                    return result
            except json.JSONDecodeError:
                pass
            
            self.logger.warning("No valid JSON found in LLM response")
            return None
            
        except Exception as e:
            self.logger.error(f"LLM extraction error: {e}")
            return None

    def _match_recipe_with_captions(self, recipe_steps, captions_segments):
        if not recipe_steps or not captions_segments:
            return recipe_steps
        
        try:
            sorted_captions = sorted(captions_segments, key=lambda x: x.get('start', 0))
            total_captions = len(sorted_captions)
            total_steps = len(recipe_steps)

            matched_recipe = []
            for i, step in enumerate(recipe_steps):
                caption_index = int((i / total_steps) * total_captions)
                
                if caption_index < total_captions:
                    start_time = sorted_captions[caption_index].get('start', 0)

                    if i + 1 < total_steps:
                        next_caption_index = int(((i + 1) / total_steps) * total_captions)
                        if next_caption_index < total_captions:
                            end_time = sorted_captions[next_caption_index].get('start', start_time + 30)
                        else:
                            end_time = start_time + 30
                    else:
                        end_time = sorted_captions[-1].get('start', start_time) + 30
                    
                    matched_step = {
                        "step": step.get("step", i + 1),
                        "instruction": step.get("instruction", ""),
                        "start_time": round(start_time, 1),
                        "end_time": round(end_time, 1)
                    }
                else:
                    matched_step = {
                        "step": step.get("step", i + 1),
                        "instruction": step.get("instruction", "")
                    }
                
                matched_recipe.append(matched_step)
            
            self.logger.info(f"Successfully matched {len(matched_recipe)} recipe steps with captions")
            return matched_recipe
            
        except Exception as e:
            self.logger.error(f"Error matching recipe with captions: {e}")
            return recipe_steps

    def _clean_extracted_data(self, video_data):
        keys_to_remove = [
            'raw_description',
            'raw_captions', 
            'clean_description',
            'clean_captions',
            'captions_segments'
        ]
        
        for key in keys_to_remove:
            video_data.pop(key, None)

        if 'metadata' in video_data:
            video_data['metadata'] = {
                'collected_at': video_data['metadata'].get('collected_at'),
                'cleaned_at': video_data['metadata'].get('cleaned_at'),
                'extracted_at': datetime.now(timezone.utc).isoformat(),
                'final_processing': True
            }
        
        return video_data

    def run(self):
        print("LLM 레시피 추출 시작...")
        
        self.db.connect()
        if not self.db.conn:
            print("데이터베이스 연결 실패")
            return

        if not self.llm:
            print("LLM API 설정 실패 - GEMINI_API_KEY 확인 필요")
            self.db.close()
            return

        videos = self.db.get_cleaned_videos()
        print(f"{len(videos)}개 비디오 추출 대상 발견")
        self.logger.info(f"Found {len(videos)} videos for LLM extraction")

        if not videos:
            print("추출할 데이터가 없습니다")
            self.db.close()
            return

        success_count = 0
        failed_count = 0
        
        for i, video in enumerate(videos, 1):
            try:
                video_data = video['data'].copy()
                video_id = video['video_id']
                
                print(f"처리 중... ({i}/{len(videos)}) {video_id}")

                clean_desc = video_data.get("clean_description", "")
                clean_captions = video_data.get("clean_captions", "")

                total_text = clean_desc + " " + clean_captions
                if len(total_text.strip()) < 100:
                    self.db.insert_skipped_video(video_id, "Insufficient cleaned text", f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                    failed_count += 1
                    print(f"스킵: {video_id} - 텍스트 부족")
                    continue
                
                source_text = f"설명: {clean_desc}\n자막: {clean_captions}"

                extracted_info = self._extract_with_llm(source_text)

                is_valid, reason = self._validate_extracted_data(extracted_info)
                
                if not is_valid:
                    self.db.insert_skipped_video(video_id, f"Quality validation failed: {reason}", f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                    failed_count += 1
                    print(f"스킵: {video_id} - {reason}")
                    self.logger.warning(f"[{video_id}] Quality validation failed: {reason}")
                    continue

                video_data.update(extracted_info)

                captions_segments = video_data.get('captions_segments', [])
                if captions_segments and extracted_info.get('recipe'):
                    matched_recipe = self._match_recipe_with_captions(
                        extracted_info['recipe'], 
                        captions_segments
                    )
                    video_data['recipe'] = matched_recipe

                cleaned_data = self._clean_extracted_data(video_data)

                if self.db.insert_or_update_video(video_id, cleaned_data):
                    success_count += 1
                    dish_name = extracted_info.get('dish_name', 'Unknown')
                    print(f"성공: {video_id} - {dish_name}")
                    self.logger.info(f"[{video_id}] Successfully extracted: {dish_name}")
                else:
                    failed_count += 1
                    print(f"저장 실패: {video_id}")
                    self.logger.error(f"[{video_id}] Failed to save extracted data")

            except Exception as e:
                video_id = video.get('video_id', 'unknown')
                failed_count += 1
                print(f"오류: {video_id} - {e}")
                self.logger.error(f"[{video_id}] Extraction error: {e}")
                try:
                    self.db.insert_skipped_video(video_id, f"Processing error: {str(e)}", f"https://www.youtube.com/watch?v={video_id}")
                    self.db.delete_video(video_id)
                except:
                    pass

        self.db.close()
        print(f"추출 완료! 성공: {success_count}개, 실패: {failed_count}개 (총 {len(videos)}개)")
        self.logger.info(f"LLM extraction finished. Success: {success_count}, Failed: {failed_count}, Total: {len(videos)}")
        
        return success_count

if __name__ == "__main__":
    extractor = LLMExtractor()
    extractor.run()