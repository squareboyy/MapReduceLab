import string
from src.core.job.mapper import Mapper

class LongWordCountMapper(Mapper): 
    def map(self, record, emit):
        translator = str.maketrans('', '', string.punctuation)
        line = str(record)
        
        for token in line.split():
            cleaned_token = token.translate(translator).lower()
            if cleaned_token and len(cleaned_token) > 5:
                emit(cleaned_token, 1)