import random
import string

class TDCom:
    @classmethod
    def get_long_name(cls, length=10, mode="letters"):
        """
        generate long name
        mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            population = string.digits
        elif mode == "letters":
            population = string.ascii_letters.lower()
        elif mode == "letters_mixed":
            population = string.ascii_letters.upper() + string.ascii_letters.lower()
        else:
            population = string.ascii_letters.lower() + string.digits
        return "".join(random.choices(population, k=length))