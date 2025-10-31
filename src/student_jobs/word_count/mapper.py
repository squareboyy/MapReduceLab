import string
from src.core.job.mapper import Mapper

class WordCountMapper(Mapper):
    def map(self, record, emit):
        # string.punctuation містить усі стандартні розділові знаки (,.!?" і т.д.)
        translator = str.maketrans('', '', string.punctuation)
        
        line = str(record)
        for token in line.split():
            cleaned_token = token.translate(translator).lower()
            if cleaned_token:
                emit(cleaned_token, 1)