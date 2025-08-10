import json
import os
from bs4 import BeautifulSoup, Comment, NavigableString
from language.processors import process_text_block, load_spacy_model
from language.validators import (
    is_pure_symbol,
    is_math_fragment,
    has_math_html_markup)
from language.detection import is_exception_language
from extractors.jsonld_extractor import extract_from_jsonld


TRANSLATABLE_TAGS = {
    "p", "span", "div", "h1", "h2", "h3", "h4", "h5", "h6",
    "label", "button", "li", "td", "th", "a", "strong", "em",
    "b", "i", "caption", "summary", "figcaption", "option", "optgroup",
    "legend", "mark", "output", "details", "time"
}

TRANSLATABLE_ATTRS = {
    "alt", "title", "placeholder", "aria-label", "aria-placeholder",
    "aria-valuetext", "aria-roledescription", "value",
    "data-i18n", "data-caption", "data-title", "data-tooltip",
    "data-label", "data-error"
}

SEO_META_FIELDS = {
    "name": {
        "description", "keywords", "robots", "author", "viewport", "theme-color"
    },
    "property": {
        "og:title", "og:description", "og:image", "og:url",
        "twitter:title", "twitter:description", "twitter:image", "twitter:card"
    }
}

SKIP_PARENTS = {
    "script", "style", "code", "pre", "noscript", "template", "svg", "canvas",
    "frameset", "frame", "noframes", "object", "embed", "base", "map"
}

BLOCKED_ATTRS = {
    "accept", "align", "autocomplete", "bgcolor", "charset", "class", "content",
    "dir", "download", "href", "id", "lang", "name", "rel", "src", "style", "type"
}

EXCLUDED_META_NAMES = {"viewport"}
EXCLUDED_META_PROPERTIES = {"og:url"}

def is_translatable_text(tag):
    """Determine if the given tag's text should be translated."""
    for parent in tag.parents:
        if parent.name and 'class' in parent.attrs and 'language-switcher' in parent.attrs['class']:
            return False

    current_element = tag.parent
    translate_override = None
    
    while current_element is not None:
        current_translate = current_element.get("translate", "").lower()
        if current_translate in {"yes", "no"}:
            translate_override = current_translate
            break
        current_element = current_element.parent

    text = tag.strip()
    if not text:
        return False

    if ((not is_exception_language(text)) and
        (is_pure_symbol(text) or is_math_fragment(text) or has_math_html_markup(tag))):
        return False

    if translate_override == "no":
        return False

    parent_tag = tag.parent.name if tag.parent else None
    default_translatable = (
        parent_tag in TRANSLATABLE_TAGS and
        parent_tag not in SKIP_PARENTS and
        not isinstance(tag, Comment))
        
    if translate_override == "yes":
        return True
        
    return default_translatable

def extract_translatable_html(input_path, lang_code, secondary_lang=None, output_dir="."):
    os.makedirs(output_dir, exist_ok=True)
    nlp = load_spacy_model(lang_code)

    with open(input_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html5lib")

    structured_output = {}
    flattened_output = {}
    block_counter = 1

    elements = list(soup.find_all(string=True))
    for element in elements:
        if is_translatable_text(element):
            text = element.strip()
            if not text:
                continue

            structured, flattened, sentence_tokens = process_text_block(f"BLOCK_{block_counter}", text, nlp)

            if sentence_tokens:
                block_id = f"BLOCK_{block_counter}"
                parent_tag = element.parent.name if element.parent else "no_parent"
                structured_output[block_id] = {"tag": parent_tag, "tokens": structured}
                flattened_output.update(flattened)
                
                replacement_content = " ".join([token[0] for token in sentence_tokens])
                if not isinstance(replacement_content, NavigableString):
                    replacement_content = NavigableString(str(replacement_content))
                element.replace_with(replacement_content)
                
                block_counter += 1

    for tag in soup.find_all():
        is_in_language_switcher = False
        for parent in tag.parents:
            if parent.name and 'class' in parent.attrs and 'language-switcher' in parent.attrs['class']:
                is_in_language_switcher = True
                break
        
        if is_in_language_switcher:
            continue
        
        for attr in TRANSLATABLE_ATTRS:
            if (attr in tag.attrs and isinstance(tag[attr], str) and attr not in BLOCKED_ATTRS):
                value = tag[attr].strip()
                if value:
                    block_id = f"BLOCK_{block_counter}"
                    structured, flattened, sentence_tokens = process_text_block(block_id, value, nlp)
                    structured_output[block_id] = {"attr": attr, "tokens": structured}
                    flattened_output.update(flattened)
                    if sentence_tokens:
                        tag[attr] = sentence_tokens[0][0]
                    block_counter += 1

    for meta in soup.find_all("meta"):
        name = meta.get("name", "").lower()
        prop = meta.get("property", "").lower()
        content = meta.get("content", "").strip()

        if name in EXCLUDED_META_NAMES or prop in EXCLUDED_META_PROPERTIES:
            continue

        if content and ((name and name in SEO_META_FIELDS["name"]) or (prop and prop in SEO_META_FIELDS["property"])):
            block_id = f"BLOCK_{block_counter}"
            structured, flattened, sentence_tokens = process_text_block(block_id, content, nlp)
            structured_output[block_id] = {"meta": name or prop, "tokens": structured}
            flattened_output.update(flattened)
            if sentence_tokens:
                meta["content"] = sentence_tokens[0][0]
            block_counter += 1

    title_tag = soup.title
    if title_tag and title_tag.string and title_tag.string.strip():
        block_id = f"BLOCK_{block_counter}"
        text = title_tag.string.strip()
        structured, flattened, sentence_tokens = process_text_block(block_id, text, nlp)
        structured_output[block_id] = {"tag": "title", "tokens": structured}
        flattened_output.update(flattened)
        if sentence_tokens:
            title_tag.string.replace_with(sentence_tokens[0][0])
        block_counter += 1

    for script_tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            raw_json = script_tag.string.strip()
            data = json.loads(raw_json)
            block_counter = extract_from_jsonld(data, block_counter, nlp, structured_output, flattened_output)
            script_tag.string.replace_with(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"⚠️ Failed to parse or process JSON-LD: {e}")
            continue

    return structured_output, flattened_output, soup