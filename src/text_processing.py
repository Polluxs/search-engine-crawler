import spacy

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

def extract_important_tokens(text, max_tokens=500):
    """Extract and prioritize important tokens using spaCy with junk filtering."""
    doc = nlp(text)

    # Custom junk words to ignore (UI scaffolding, marketing, filler)
    JUNK_WORDS = {
        "account", "login", "signup", "subscribe", "sign", "register", "create", "click",
        "platform", "solution", "experience", "support", "discount", "offers", "order",
        "shop", "app", "center", "categories", "policy", "privacy", "help", "b2b", "search",
        "value", "promotion", "delivery", "products", "production", "contact", "username",
        "password", "terms", "conditions", "newsletter", "settings", "mobile", "website",
        "visit", "start", "email"
    }

    # Priority 1: Named entities (excluding numeric junk)
    entities = [
        ent.text.strip()
        for ent in doc.ents
        if len(ent.text.strip()) > 2 and not any(char.isdigit() for char in ent.text)
    ]

    # Priority 2a: Noun phrases (filtered and meaningful)
    noun_phrases = [
        chunk.text.strip().lower()
        for chunk in doc.noun_chunks
        if (
            len(chunk.text.split()) > 1 and
            len(chunk.text) > 3 and
            not any(tok.lemma_.lower() in JUNK_WORDS for tok in chunk)
        )
    ]

    # Priority 2b: Important nouns and proper nouns
    important_nouns = [
        token.lemma_.lower()
        for token in doc
        if (
            token.pos_ in ["NOUN", "PROPN"] and
            not token.is_stop and
            not token.is_punct and
            len(token.text) > 2 and
            token.lemma_.lower() not in JUNK_WORDS
        )
    ]

    # Priority 3: Adjectives and verbs (meaningful ones only)
    descriptors = [
        token.lemma_.lower()
        for token in doc
        if (
            token.pos_ in ["ADJ", "VERB"] and
            not token.is_stop and
            not token.is_punct and
            len(token.text) > 3 and
            token.lemma_.lower() not in JUNK_WORDS
        )
    ]

    # Combine and deduplicate while preserving order
    all_tokens = (
        entities[:20] +
        noun_phrases[:30] +
        list(dict.fromkeys(important_nouns))[:40] +
        list(dict.fromkeys(descriptors))[:20]
    )

    seen = set()
    unique_tokens = []
    for token in all_tokens:
        norm = token.lower().strip()
        if norm not in seen and len(norm) > 2:
            seen.add(norm)
            unique_tokens.append(token.strip())

    return unique_tokens[:max_tokens]


def detect_has_comments(page_html):
    """Detect if the page has a comment system."""
    comment_indicators = [
        "comment", "reply", "discuss", "disqus", "livefyre", 
        "facebook comment", "commento", "utterances"
    ]
    html_lower = page_html.lower()
    return any(indicator in html_lower for indicator in comment_indicators)