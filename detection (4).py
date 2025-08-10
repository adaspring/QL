import regex as re


SPACY_MODELS = {
    "en": "en_core_web_sm",
    "zh": "zh_core_web_sm",
    "fr": "fr_core_news_sm",
    "es": "es_core_news_sm",
    "de": "de_core_news_sm",
    "it": "it_core_news_sm",
    "pt": "pt_core_news_sm",
    "ru": "ru_core_news_sm",
    "el": "el_core_news_sm",
    "ca": "es_core_news_sm",
    "eu": "xx_ent_wiki_sm",
    "xx": "xx_ent_wiki_sm"  # fallback for multilingual
}


def is_exception_language(text):
    """
    Detect if the text contains a script or pattern matching a non-default language.

    Returns:
        A language code (e.g. 'zh', 'fr', 'ru', 'xx') if a match is found.
        Returns None if no exception language is detected.
    """
    if contains_chinese(text):
        return "zh"
    elif contains_arabic(text):
        return "xx"
    elif contains_hebrew(text):
        return "xx"
    elif contains_thai(text):
        return "xx"
    elif contains_devanagari(text):
        return "xx"
    return None


def detectis_exception_language(text):
    """
    Detect if the text contains a script or pattern matching a non-default language.

    Returns:
        A language code (e.g. 'zh', 'fr', 'ru', 'xx') if a match is found.
        Returns None if no exception language is detected.
    """
    if contains_chinese(text):
        return "zh"
    elif contains_english(text):
        return "en"
    elif contains_euskera(text):   
        return "eu"
    elif contains_catalan(text):   
        return "ca"
    elif contains_arabic(text):
        return "xx"
    elif contains_cyrillic(text):
        return "ru"
    elif contains_greek(text):
        return "el"
    elif contains_hebrew(text):
        return "xx"
    elif contains_thai(text):
        return "xx"
    elif contains_devanagari(text):
        return "xx"
    elif contains_french(text):
        return "fr"
    elif contains_spanish(text):
        return "es"
    elif contains_italian(text):
        return "it"
    elif contains_german(text):
        return "de"
    return None


def contains_chinese(text):
    return re.search(r'[\u4e00-\u9fff]', text) is not None


def contains_arabic(text):
    return re.search(r'[\u0600-\u06FF]', text) is not None


def contains_cyrillic(text):
    return re.search(r'[\u0400-\u04FF]', text) is not None


def contains_greek(text):
    return re.search(r'[\u0370-\u03FF]', text) is not None


def contains_hebrew(text):
    return re.search(r'[\u0590-\u05FF]', text) is not None


def contains_thai(text):
    return re.search(r'[\u0E00-\u0E7F]', text) is not None


def contains_devanagari(text):
    return re.search(r'[\u0900-\u097F]', text) is not None


def contains_french(text):
    return (
        re.search(r'[àâæçéèêëîïôœùûüÿ]', text, re.IGNORECASE) is not None or
        re.search(r'\b(le|la|les|un|une|des|ce|cette|est|avec|mais|pour|pas|qui|sur)\b', text, re.IGNORECASE) is not None
    )


def contains_spanish(text):
    return (
        re.search(r'[áéíóúüñ]', text, re.IGNORECASE) is not None or
        re.search(r'\b(el|la|los|las|un|una|que|es|con|pero|por|para|cómo|sin|más)\b', text, re.IGNORECASE) is not None
    )


def contains_italian(text):
    return (
        re.search(r'[àèéìíîòóùú]', text, re.IGNORECASE) is not None or
        re.search(r'\b(il|lo|la|gli|le|un|una|che|è|con|ma|come|perché|senza|più|meno)\b', text, re.IGNORECASE) is not None
    )


def contains_portuguese(text):
    return (
        re.search(r'[áàâãéêíóôõúç]', text, re.IGNORECASE) is not None or
        re.search(r'\b(o|a|os|as|um|uma|que|é|com|mas|por|para|como|sem|mais)\b', text, re.IGNORECASE) is not None
    )


def contains_german(text):
    return (
        re.search(r'[äöüß]', text, re.IGNORECASE) is not None or
        re.search(r'\b(der|die|das|ein|eine|ist|mit|aber|und|nicht|für|ohne|warum|wie|mehr)\b', text, re.IGNORECASE) is not None
    )


def contains_english(text):
    return (
        re.search(r'\b(the|and|is|of|to|in|with|but|not|a|an|for|on|that|how|without|more)\b', text, re.IGNORECASE) is not None
    )

def contains_catalan(text):
    return (
        re.search(r'[àèéíòóúüç·]', text, re.IGNORECASE) is not None or
        re.search(r'\bny\b|\bll\b|\btx\b|\bix\b', text, re.IGNORECASE) is not None or
        # Common Catalan words that differ from Spanish
        re.search(r'\b(el|la|els|les|un|una|que|és|amb|però|per|com|sense|més|menys)\b', text, re.IGNORECASE) is not None or
        re.search(r'\b(aquesta|aquest|això|aquí|allà|només|també|molt|poc|gent)\b', text, re.IGNORECASE) is not None or
        # Catalan-specific verb forms
        re.search(r'\b(som|sou|són|hem|heu|han|vaig|vas|va|vam|vau|van)\b', text, re.IGNORECASE) is not None or
        # Distinctive Catalan words
        re.search(r'\b(català|barceloní|girona|lleida|tarragona|valencia)\b', text, re.IGNORECASE) is not None
    )

def contains_euskera(text):
    return (
        re.search(r'tx|tz|ts|rr|ll|ñ', text, re.IGNORECASE) is not None or
        # Common Basque suffixes (agglutination markers)
        re.search(r'(ak|ek|ok|uk|an|en|on|un|ra|re|ro|ru|tik|etik|otik|utik|ko|eko|oko|uko)$', text, re.IGNORECASE) is not None or
        # Common Basque words
        re.search(r'\b(eta|baina|edo|hau|hori|hura|nik|hik|guk|zuek|haiek)\b', text, re.IGNORECASE) is not None or
        re.search(r'\b(izan|egon|ukan|egin|joan|etorri|ikusi|entzun|esan|jakin)\b', text, re.IGNORECASE) is not None or
        # Basque numbers and common expressions
        re.search(r'\b(bat|bi|hiru|lau|bost|sei|zazpi|zortzi|bederatzi|hamar)\b', text, re.IGNORECASE) is not None or
        # Distinctive Basque place names and terms
        re.search(r'\b(euskera|euskadi|bilbao|donostia|gasteiz|iruña|baiona)\b', text, re.IGNORECASE) is not None or
        # Basque-specific grammatical particles
        re.search(r'\b(da|dira|du|dute|dago|daude|dator|datoz)\b', text, re.IGNORECASE) is not None
    )