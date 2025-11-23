"""
Number to Emoji mapping
"""
NUMBER_EMOJIS = {
    '0': '0️⃣', '1': '1️⃣', '2': '2️⃣', '3': '3️⃣', 
    '4': '4️⃣', '5': '5️⃣', '6': '6️⃣', '7': '7️⃣', 
    '8': '8️⃣', '9': '9️⃣'
}

def convert_to_emoji_numbers(digits: List[int]) -> str:
    """Convert list of digits to emoji string"""
    return " ".join([NUMBER_EMOJIS[str(d)] for d in digits])

def convert_string_to_emoji(text: str) -> str:
    """Convert string containing numbers to emojis"""
    result = ""
    for char in text:
        if char in NUMBER_EMOJIS:
            result += NUMBER_EMOJIS[char]
        else:
            result += char
    return result
