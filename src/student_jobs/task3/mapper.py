import string
from src.core.job.mapper import Mapper

VOWELS = "аеєиіїоуюя" + "aeiou"
CONSONANTS = "бвгґджзйклмнпрстфхцчшщ" + "bcdfghjklmnpqrstvwxyz"

class VowelConsonantMapper(Mapper):
    def map(self, record, emit):
        translator = str.maketrans('', '', string.punctuation)
        line = str(record)
        
        for token in line.split():
            cleaned_token = token.translate(translator).lower()
            
            if not cleaned_token:
                continue
                
            word_len = len(cleaned_token)
            v_count = 0
            c_count = 0
            
            for char in cleaned_token:
                if char in VOWELS:
                    v_count += 1
                elif char in CONSONANTS:
                    c_count += 1
            
            if v_count > 0 or c_count > 0:
                emit(word_len, (v_count, c_count))