import sys
import argparse
import asyncio
from pathlib import Path

project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.collect.youtube_crawler import YoutubeCrawler
from src.clean.text_cleaner import TextCleaner
from src.extract.llm_extractor import LLMExtractor
from utils.config import Config
from utils.logger import Logger

class RecipeCrawlerPipeline:
    """요리 레시피 크롤링 파이프라인 관리 클래스"""
    
    def __init__(self):
        self.logger = Logger("main_pipeline.log", Config.LOG_LEVEL).get_logger()
        
    def run_collection(self, keyword: str):
        print("=" * 50)
        print("1단계: 데이터 수집")
        print("=" * 50)
        
        crawler = YoutubeCrawler(keyword)
        collected_count = asyncio.run(crawler.run())
        
        print(f"수집 완료: {collected_count}개 비디오")
        self.logger.info(f"Collection completed: {collected_count} videos")
        return collected_count
    
    def run_cleaning(self):
        print("\n" + "=" * 50)
        print("2단계: 텍스트 정제")
        print("=" * 50)
        
        cleaner = TextCleaner()
        cleaned_count = cleaner.run()
        
        print(f"정제 완료: {cleaned_count}개 비디오")
        self.logger.info(f"Cleaning completed: {cleaned_count} videos")
        return cleaned_count
    
    def run_extraction(self):
        print("\n" + "=" * 50)
        print("3단계: 레시피 추출")
        print("=" * 50)
        
        extractor = LLMExtractor()
        extracted_count = extractor.run()
        
        print(f"추출 완료: {extracted_count}개 비디오")
        self.logger.info(f"Extraction completed: {extracted_count} videos")
        return extracted_count
    
    def run_full_pipeline(self, keyword: str):
        print("YouTube 요리 레시피 크롤링 파이프라인 시작")
        print(f"키워드: {keyword}")
        
        try:
            collected_count = self.run_collection(keyword)
            if collected_count == 0:
                print("수집된 데이터가 없어서 파이프라인을 중단합니다.")
                return
            
            cleaned_count = self.run_cleaning()
            if cleaned_count == 0:
                print("정제할 데이터가 없습니다.")
                return
            
            extracted_count = self.run_extraction()
            
            print("\n" + "=" * 50)
            print("파이프라인 완료!")
            print("=" * 50)
            print(f"수집: {collected_count}개")
            print(f"정제: {cleaned_count}개")
            print(f"최종 추출: {extracted_count}개")
            print(f"성공률: {(extracted_count/collected_count*100):.1f}%" if collected_count > 0 else "0%")
            
            self.logger.info(f"Full pipeline completed for '{keyword}': {collected_count} → {cleaned_count} → {extracted_count}")
            
        except KeyboardInterrupt:
            print("\n사용자에 의해 중단되었습니다.")
        except Exception as e:
            print(f"\n파이프라인 실행 중 오류: {e}")
            self.logger.error(f"Pipeline error: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="YouTube Recipe Crawler Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 전체 실행
  python main.py full "김치찌개 레시피"
  
  # 개별 단계 실행
  python main.py collect "불고기 만들기"
  python main.py clean
  python main.py extract
        """
    )
    
    parser.add_argument("command", choices=["full", "collect", "clean", "extract"], 
                       help="실행할 작업")
    parser.add_argument("keyword", nargs="?", help="검색 키워드 (collect, full 명령어에서 필요)")
    
    args = parser.parse_args()
    
    pipeline = RecipeCrawlerPipeline()
    
    if args.command == "full":
        if not args.keyword:
            print("full 명령어는 키워드가 필요합니다.")
            print("사용법: python main.py full \"김치찌개 레시피\"")
            sys.exit(1)
        pipeline.run_full_pipeline(args.keyword)
        
    elif args.command == "collect":
        if not args.keyword:
            print("collect 명령어는 키워드가 필요합니다.")
            print("사용법: python main.py collect \"불고기 만들기\"")
            sys.exit(1)
        pipeline.run_collection(args.keyword)
        
    elif args.command == "clean":
        pipeline.run_cleaning()
        
    elif args.command == "extract":
        pipeline.run_extraction()

if __name__ == "__main__":
    main()