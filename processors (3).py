import sys
import spacy
import subprocess
from pypinyin import lazy_pinyin
from .detection import SPACY_MODELS, detectis_exception_language, contains_chinese


def load_spacy_model(lang_code):
    if lang_code not in SPACY_MODELS:
        print(f"Unsupported language '{lang_code}'. Choose from: {', '.join(SPACY_MODELS)}.")
        sys.exit(1)

    model_name = SPACY_MODELS[lang_code]

    try:
        nlp = spacy.load(model_name)
    except OSError:
        print(f"spaCy model '{model_name}' not found. Downloading automatically...")
        subprocess.run(["python", "-m", "spacy", "download", model_name], check=True)
        nlp = spacy.load(model_name)

    # Minimal addition: ensure sentence segmentation
    if "parser" not in nlp.pipe_names and "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer", first=True)

    return nlp


def process_text_block(block_id, text, default_nlp):
    lang_code = detectis_exception_language(text)
    nlp = default_nlp if not lang_code else load_spacy_model(lang_code)
    detected_language = lang_code or "default"
    
    structured = {}
    flattened = {}
    sentence_tokens = []

    doc = nlp(text)
    for s_idx, sent in enumerate(doc.sents, 1):
        s_key = f"S{s_idx}"
        sentence_id = f"{block_id}_{s_key}"
        sentence_text = sent.text
        flattened[sentence_id] = sentence_text
        structured[s_key] = {"text": sentence_text, "words": {}}
        sentence_tokens.append((sentence_id, sentence_text))

        for w_idx, token in enumerate(sent, 1):
            w_key = f"W{w_idx}"
            word_id = f"{sentence_id}_{w_key}"
            flattened[word_id] = token.text
            structured[s_key]["words"][w_key] = {  # Keep `{` on the same line
               "text": token.text,
               "pos": token.pos_,
               "language": detected_language,
               "ent": token.ent_type_ or None,
               "pinyin": (
                  " ".join(lazy_pinyin(token.text)) 
                  if contains_chinese(token.text) 
                  else None
               )
            }

    return structured, flattened, sentence_tokens