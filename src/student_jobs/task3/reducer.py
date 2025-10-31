from src.core.job.reducer import Reducer

class VowelConsonantReducer(Reducer):
    def reduce(self, key, values, emit):  
        total_vowels = 0
        total_consonants = 0
        
        for v_count, c_count in values:
            total_vowels += v_count
            total_consonants += c_count
            
        total_letters = total_vowels + total_consonants
        
        if total_letters == 0:
            return
  
        vowel_percent = (total_vowels / total_letters) * 100
        consonant_percent = (total_consonants / total_letters) * 100
        
        output_key = f"{key} символів"
        output_value = (
            f"{vowel_percent:.0f}% голосних, "
            f"{consonant_percent:.0f}% приголосних"
        )
        
        emit(output_key, output_value)