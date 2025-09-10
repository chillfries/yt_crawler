import re

def remove_links_and_hashtags(text):
    """URL과 해시태그를 제거합니다."""
    if not text:
        return ""
    
    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    # Remove hashtags
    text = re.sub(r'#\S+', '', text)
    return text

def join_sentences(text_list):
    """텍스트 리스트를 하나의 문자열로 결합합니다."""
    if not text_list:
        return ""
    
    # yt-dlp에서 추출한 자막 형식 처리
    if isinstance(text_list, list) and len(text_list) > 0:
        if isinstance(text_list[0], dict):
            # yt-dlp 자막 형식: [{'text': '...', 'start': 0.0, 'duration': 1.0}, ...]
            sentences = []
            for item in text_list:
                if 'text' in item and item['text'].strip():
                    sentences.append(item['text'].strip())
            return " ".join(sentences)
        elif isinstance(text_list[0], str):
            # 일반 문자열 리스트
            return " ".join(text_list)
    
    return ""

def join_captions_from_ytdlp(captions_list):
    """yt-dlp에서 추출한 자막 리스트를 문자열로 변환합니다."""
    if not captions_list:
        return ""
    
    text_parts = []
    for caption in captions_list:
        if isinstance(caption, dict) and 'text' in caption:
            text = caption['text'].strip()
            if text:
                text_parts.append(text)
    
    return " ".join(text_parts)

def remove_redundant_spaces(text):
    """중복된 공백을 제거합니다."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def clean_subtitle_text(text):
    """자막에서 특수 문자나 불필요한 기호를 정리합니다."""
    if not text:
        return ""
    
    # 자막에서 자주 나타나는 패턴들 제거
    text = re.sub(r'\[.*?\]', '', text)  # [음악], [박수] 등 제거
    text = re.sub(r'\(.*?\)', '', text)  # (웃음), (박수) 등 제거
    text = re.sub(r'♪.*?♪', '', text)    # 음악 기호 제거
    text = re.sub(r'[♪♫♬♩]', '', text)   # 음악 관련 유니코드 제거
    
    # 특수 문자 정리 (필요한 구두점은 유지)
    text = re.sub(r'[^\w\s가-힣.,!?:;-]', '', text)
    
    return text.strip()

def merge_short_sentences(text, min_length=10):
    """너무 짧은 문장들을 병합합니다."""
    if not text:
        return ""
    
    sentences = re.split(r'[.!?]+', text)
    merged = []
    current = ""
    
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
            
        if len(current) < min_length:
            current += " " + sentence if current else sentence
        else:
            if current:
                merged.append(current)
            current = sentence
    
    if current:
        merged.append(current)
    
    return ". ".join(merged) + "." if merged else ""