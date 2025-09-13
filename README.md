pip install -r requirements.txt

# 전체 실행
python main.py full "김치찌개 레시피"

# 개별 단계 실행
python main.py collect "불고기 만들기"  # 수집만
python main.py clean                   # 정제만  
python main.py extract                 # 추출만