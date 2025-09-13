import re

def remove_links_and_hashtags(text):
    if not text:
        return ""
    
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'#\S+', '', text)
    return text

def join_sentences(text_list):
    if not text_list:
        return ""
    
    if isinstance(text_list, list) and len(text_list) > 0:
        if isinstance(text_list[0], dict):
            sentences = []
            for item in text_list:
                if 'text' in item and item['text'].strip():
                    sentences.append(item['text'].strip())
            return " ".join(sentences)
        elif isinstance(text_list[0], str):
            return " ".join(text_list)
    
    return ""

def join_captions_from_ytdlp(captions_list):
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
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def clean_subtitle_text(text):
    if not text:
        return ""
    
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'♪.*?♪', '', text)
    text = re.sub(r'[♪♫♬♩]', '', text)
    
    text = re.sub(r'[하호히히흐][하호히히흐]{1,}', '', text)
    text = re.sub(r'으(잉|악)', '', text)
    text = re.sub(r'[오아우으][오아우으]{1,}', '', text)
    
    text = re.sub(r'[^\w\s가-힣.,!?:;-]', '', text)
    
    return text.strip()

def merge_short_sentences(text, min_length=10):
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

def remove_conversational_fillers(text):
    if not text:
        return ""
    
    fillers = [
        "안녕하세요", "여러분", "오늘", "시작", "이번 시간", "만들어 볼게요", "제가",
        "자", "그럼", "이제", "먼저", "다음은"
    ]
    
    pattern = r'\b(' + '|'.join(fillers) + r')\b'
    cleaned_text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    sentences = re.split('([.!?])', cleaned_text)
    if sentences and len(sentences) > 1:
        if len(sentences[0]) < 20:
            cleaned_text = "".join(sentences[2:]).strip()
    
    return cleaned_text