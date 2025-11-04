from collections import Counter
from langdetect.lang_detect_exception import LangDetectException
from langdetect import detect
import trafilatura
import re
import hashlib
from datasketch import MinHash, MinHashLSH
import pandas as pd


def repetitiveness_score(text, n=3):
    """
    Split text to n-grams, count duplicates and divided by total n-gram count
    """

    words = text.split()
    if len(words) < n:
        return 0.0
    ngrams = [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]
    counts = Counter(ngrams)
    total = len(ngrams)
    repeated = sum(v for v in counts.values() if v > 1)
    return repeated / total

def detect_language(text):
    """
    Use langdetect to return the language of text and unknown if no language
    """
    try:
        return detect(text)
    except LangDetectException:
        return "Unknown"
    
def clean_html_trafilatura(text):
    """
    Using trafilatura library clean html elements
    """
    extracted = trafilatura.extract(text)
    return extracted if extracted else text

def clean_special_characters(text: str) -> str:
    """
    Function to clean special characters from text and normalise whitespace
    """
    text = re.sub(r'[âÂÃ¢€‹„”¢¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿]', '', text)
    #text = re.sub(r'\s+', ' ', text).strip()
    # REMOVED whitespace stripping as we need paragraphs for fuzzy deduplication
    #Remove general symbol ranges (e.g., currency, dingbats, box drawings)
    text= re.sub(r'[\u20A0-\u20CF\u2100-\u214F\u2190-\u21FF\u2500-\u257F\u2580-\u259F]', '', text)
    return text

def mask_urls(text: str) -> str:
    """
    Take text containing url strings and return the same string with the url masked as [URL]
    """
    text = re.sub(r'\b((?:https?:\/\/)?(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\/[^\s]*)?)\b', "[URL]",text)
    return text

def flag_toxic_keywords(text):
    """
    Flag based on list of dirty naughty obscene  and otherwise bad words (english)
    https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words
    """
    try:
        with open('../../data/raw/en.txt', 'r', encoding='utf-8') as f:
            bad_words = [line.strip() for line in f if line.strip()]

        pattern = re.compile(r'\b(' + '|'.join(map(re.escape, bad_words)) + r')\b', flags=re.IGNORECASE)
        return bool(pattern.search(text))
    except Exception as e:
        print("ERROR in flag_toxic_keywords:", e)
        return False
    
def mask_text(text):
    phone = re.compile(r'\b(?:\+?61|0)[2-478](?:[ -]?\d){8}\b')
    email = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')
    tfn = re.compile(r'\b\d{3}\s?\d{3}\s?\d{3}\b')

    masked_items = []

    if re.search(email, text):
        text = re.sub(email, "[EMAIL_MASKED]", text)
        masked_items.append("email")

    if re.search(phone, text):
        text = re.sub(phone, "[PHONE_MASKED]", text)
        masked_items.append("phone")

    if re.search(tfn, text):
        text = re.sub(tfn, "[TFN_MASKED]", text)
        masked_items.append("tfn")

    return text, ", ".join(masked_items) if masked_items else None

def hash_text(text):
    """
    Apply exact hashing to df
    """
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def assign_shard(fp, n_shards=8):
    """Assign text to a shard based on hashing"""
    return int(fp, 16) % n_shards

def shard_dataframe(df, n_shards=8):
    # Normalise and fingerprint
    df['shard'] = df['hashing'].apply(lambda x: assign_shard(x, n_shards))
    return df

def create_minhash(text):
        # TO DO REWRITE TO TAKE NUM PERM IN FUNCTION
        m = MinHash(num_perm=128)
        for word in text.lower().split():
            m.update(word.encode('utf-8'))
        return m

def split_paragraphs(df):
    # create a docindex for split
    df['doc_id'] = df.index
    all_paragraphs = []

    for idx, row in df.iterrows():
        doc_id = row['doc_id']
        text = row['text']
        url = row['url']


        # split by para
        paragraphs = text.split("\n\n")

        # non empty paras
        paragraphs = [para.strip() for para in paragraphs if para.strip()]
        # useing a dict store para info in all_paragraphs
        # paragraph id important for order within docs
        for i, para in enumerate(paragraphs):
            all_paragraphs.append({
                'doc_id': doc_id,
                'paragraph_id': i,
                'paragraph_text': para,
                'url': url
                })
    df_paragraphs = pd.DataFrame(all_paragraphs)
    return df_paragraphs

def general_validations(df:pd.DataFrame):
    """
    Some general df validations that will run on a dataframe
    """
    stats = {}
    nullsintxt = int(df['text'].isna().sum())
    stats['Nulls in text data'] = nullsintxt

    # html checking
    df["element_count"] = df["text"].apply(count_html_tags)
    stats['Html tags'] = int(df['element_count'].sum())

    # utf8 encoding checking
    df['non-utf8_count'] = df["text"].apply(count_non_utf8_chars)
    stats['Utf8 chars'] = int(df['non-utf8_count'].sum())

    return stats

def count_html_tags(text):
    """
    simple regex to get a general sense of the amt of html tags in text
    """
    TAG_REGEX = re.compile(r"<\s*/?\s*([a-zA-Z0-9]+)[^>]*>") # general to match html tags
    return len(TAG_REGEX.findall(text))

def count_non_utf8_chars(text):
    """
    Return count of characters which cant be encoded to utf8
    """
    count = 0
    for c in text:
        try:
            c.encode('utf-8')
        except UnicodeEncodeError:
            count += 1
    return count