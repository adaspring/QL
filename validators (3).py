import regex as re


def is_pure_symbol(text):
    """Skip text with no alphabetic characters."""
    return not re.search(r'[A-Za-z]', text)


def is_symbol_heavy(text):
    """Skip only if there's zero real words and many symbols (multilingual safe)."""

    # Count real words of 3+ letters
    words = re.findall(r'\b\p{L}{3,}\b', text)
    word_count = len(words)

    # If there's at least one real word, it's not symbol-heavy
    if word_count > 0:
        return False

    # Otherwise check for excessive symbols
    symbol_count = len(re.findall(r'[\p{P}\p{S}\d_]', text))
    return symbol_count > 0  # treat as symbol-heavy if only symbols


def has_real_words(text):
    return re.search(r'\b\p{L}{3,}\b', text, re.UNICODE) is not None


def has_math_html_markup(element):
    """Check for math-specific HTML markup (MathML, LaTeX, etc.)."""
    parent = element.parent
    return (
        parent.name == 'math' or 
        re.search(r'\$.*?\$|\\\(.*?\\\)', parent.text or '') or
        any(cls in parent.get('class', []) for cls in ['math', 'equation', 'formula'])
    )


def is_math_fragment(text):
    """Check if text is a math formula without lexical words."""
    equation_pattern = r'''
        (\w+\s*[=+\-*/^]\s*\S+)|  # Equations like "x = y+1"
        (\d+[\+\-\*/]\d+)|         # Arithmetic "2+3"
        ([a-zA-Z]+\^?\d+)|         # Exponents "x²"
        (\$.*?\$|\\\(.*?\\\))      # LaTeX "$E=mc^2$"
    '''
    has_math = re.search(equation_pattern, text, re.VERBOSE)
    return (has_math and not has_real_words(text)) or is_symbol_heavy(text)  # <-- Fixed line continuation