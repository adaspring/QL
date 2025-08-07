import os
import json
import deepl
import argparse
import regex as re 
import hashlib
from pathlib import Path
from collections import defaultdict



def track_memory_usage(
    translation_memory, 
    original_texts, 
    translatable_map, 
    output_dir, 
    input_filename,
    all_segments_info
):
    """
    Track and save memory usage statistics for this file
    
    Args:
        translation_memory: The current translation memory dict
        original_texts: Dict mapping tokens to (text, memory_key) tuples
        translatable_map: Dict mapping tokens to translated text
        output_dir: Directory to save stats
        input_filename: Name of input file being processed
        all_segments_info: Dict with complete segment info including cache hit status
    """
    
    # Calculate cache hits and misses
    cache_hits = 0
    cache_misses = 0
    
    for token, (text, memory_key, was_cache_hit) in all_segments_info.items():
        if was_cache_hit:
            cache_hits += 1
        else:
            cache_misses += 1
    
    total_segments = len(all_segments_info)
    hit_rate = (cache_hits / total_segments * 100) if total_segments > 0 else 0
    memory_size = len(translation_memory)

    
    
    # Create stats dictionary
    stats = {
        'file': input_filename,
        'timestamp': str(Path(input_filename).stat().st_mtime) if Path(input_filename).exists() else "unknown",
        'total_segments': total_segments,
        'cache_hits': cache_hits,
        'cache_misses': cache_misses,
        'hit_rate_percent': round(hit_rate, 2),
        'memory_size_after': memory_size,
        'new_entries_added': cache_misses,  # New entries = cache misses
        'processing_order': 0  # Will be updated in batch stats
    }
    
    # Save individual file stats
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_basename = Path(input_filename).stem
    stats_file = output_path / f"{file_basename}_memory_stats.json"
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    # Update batch stats file (shared across all files)
    batch_stats_file = output_path.parent / "memory_usage_batch.json"
    
    if batch_stats_file.exists():
        with open(batch_stats_file, 'r', encoding='utf-8') as f:
            batch_data = json.load(f)
    else:
        batch_data = {
            'processing_order': [],
            'cumulative_stats': [],
            'summary': {}
        }
    
    # Update processing order
    stats['processing_order'] = len(batch_data['processing_order']) + 1
    batch_data['processing_order'].append(input_filename)
    batch_data['cumulative_stats'].append(stats)
    
    # Calculate summary
    total_files = len(batch_data['cumulative_stats'])
    total_segments_all = sum(s['total_segments'] for s in batch_data['cumulative_stats'])
    total_hits_all = sum(s['cache_hits'] for s in batch_data['cumulative_stats'])
    total_misses_all = sum(s['cache_misses'] for s in batch_data['cumulative_stats'])
    
    batch_data['summary'] = {
        'total_files_processed': total_files,
        'total_segments_processed': total_segments_all,
        'total_cache_hits': total_hits_all,
        'total_cache_misses': total_misses_all,
        'overall_hit_rate_percent': round((total_hits_all / total_segments_all * 100) if total_segments_all > 0 else 0, 2),
        'final_memory_size': memory_size,
        'last_updated': str(Path().cwd())  # Simple timestamp
    }
    
    # Save batch stats
    with open(batch_stats_file, 'w', encoding='utf-8') as f:
        json.dump(batch_data, f, indent=2, ensure_ascii=False)
    
    # Print stats to console
    print(f"📊 Memory stats for {Path(input_filename).stem}:")
    print(f"   Segments: {total_segments:,} | Hits: {cache_hits:,} ({hit_rate:.1f}%) | Misses: {cache_misses:,}")
    print(f"   Memory size: {memory_size:,} entries")
    
    return stats

def create_content_hash(text):
    """Create a consistent hash for content-based memory keys"""
    # Normalize text for consistent hashing
    normalized = re.sub(r'\s+', ' ', text.strip().lower())
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()[:12]


def create_efficient_translatable_map(
    json_data, 
    translator, 
    target_lang="FR", 
    primary_lang=None, 
    secondary_lang=None, 
    memory_file=None,
    update_memory=False,
    metrics=None
):
    """
    Creates a translation map with language validation and improved memory.
    Memory keys are now content-based hashes to identify identical blocks across files.
    """
    if target_lang == "PT":
        target_lang = "PT-PT"
    elif target_lang == "EN":
        target_lang = "EN-US"
    # Load existing memory
    translation_memory = {}
    initial_memory_size = 0
    if memory_file and os.path.exists(memory_file):
        try:
            with open(memory_file, 'r', encoding='utf-8') as f:
                translation_memory = json.load(f)
            initial_memory_size = len(translation_memory)  # NEW LINE
            print(f"🧠 Loaded {len(translation_memory)} cached translations from memory")
        except json.JSONDecodeError:
            print(f"⚠️  Corrupted memory file - resetting {memory_file}")
            translation_memory = {}
            # Auto-recover by recreating
            with open(memory_file, 'w', encoding='utf-8') as f:
                json.dump({}, f)   

    
    # Prepare translation data structures
    translatable_map = {}
    texts_to_translate = []
    token_indices = []
    original_texts = {}
    cache_hits = 0
    cache_misses = 0

    all_segments_info = {}

    # Process all blocks and segments
    for block_id, block_data in json_data.items():
        if "text" in block_data:
            text = block_data["text"]
            token = block_id
            
            # Create content-based memory key (improved for block deduplication)
            content_hash = create_content_hash(text)
            memory_key = f"{primary_lang or 'any'}-{target_lang}:hash:{content_hash}"
            
            if memory_key in translation_memory:
                translatable_map[token] = translation_memory[memory_key]
                cache_hits += 1
                all_segments_info[token] = (text, memory_key, True)
                if metrics: metrics["cache_hits"] += 1
                print(f"💾 Cache hit for block: {token}")
            else:
                texts_to_translate.append(text)
                token_indices.append(token)
                original_texts[token] = (text, memory_key)
                cache_misses += 1
                all_segments_info[token] = (text, memory_key, False)
                if metrics: 
                    metrics["cache_misses"] += 1
                    metrics["total_characters"] += len(text) 
            
        if "segments" in block_data:
            for segment_id, segment_text in block_data["segments"].items():
                token = f"{block_id}_{segment_id}"
                
                # Create content-based memory key for segments
                content_hash = create_content_hash(segment_text)
                memory_key = f"{primary_lang or 'any'}-{target_lang}:hash:{content_hash}"
                
                if memory_key in translation_memory:
                    translatable_map[token] = translation_memory[memory_key]
                    cache_hits += 1
                    all_segments_info[token] = (segment_text, memory_key, True)
                    if metrics: metrics["cache_hits"] += 1
                    print(f"💾 Cache hit for segment: {token}")
                else:
                    texts_to_translate.append(segment_text)
                    token_indices.append(token)
                    original_texts[token] = (segment_text, memory_key)
                    cache_misses += 1
                    all_segments_info[token] = (segment_text, memory_key, False)
                    if metrics: metrics["cache_misses"] += 1
                
                # Track ALL characters processed (both hits and misses)
                if metrics:
                    metrics["total_characters"] += len(segment_text)
                    

    print(f"📊 Memory statistics: {cache_hits} hits, {cache_misses} misses")
    if cache_hits + cache_misses > 0:
        hit_rate = (cache_hits / (cache_hits + cache_misses)) * 100
        print(f"📊 Cache hit rate: {hit_rate:.1f}%")
    
    def clean_text(text):
        """Clean text for language detection"""
        text = re.sub(r'^(.*?):\s*', '', text)
        text = re.sub(r'[^\p{L}\p{N}\s=+-]', ' ', text, flags=re.UNICODE)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:500]
        
    # Language-aware batch translation
    if texts_to_translate:
        print(f"🌐 Processing {len(texts_to_translate)} new segments with language validation...")
        
        batch_size = 330
        translations_added = 0
        
        for batch_idx in range(0, len(texts_to_translate), batch_size):
            batch = texts_to_translate[batch_idx:batch_idx+batch_size]
            translated_batch = []
            detected_languages_batch = []
        
            try:
                # Phase 1: Language detection with cleaned text
                detection_texts = [clean_text(text) for text in batch]
                translation_texts = batch  # Keep original texts for translation
                
                detection_results = translator.translate_text(
                    detection_texts,
                    target_lang=target_lang,
                    preserve_formatting=True
                )
                if metrics: metrics["api_calls"] += 1

                # Phase 2: Translation with original texts
                for idx, detection in enumerate(detection_results):
                    detected_lang = detection.detected_source_lang.lower()
                    allowed_langs = {lang.lower() for lang in [primary_lang, secondary_lang] if lang}
                    original_text = translation_texts[idx]
                    

                    if allowed_langs and detected_lang in allowed_langs:
                        result = translator.translate_text(original_text, target_lang=target_lang)
                        translated_batch.append(result.text)
                        detected_languages_batch.append(detected_lang)
                        translations_added += 1
                    else:
                        # Keep original text if language not in allowed list
                        translated_batch.append(original_text)
                        detected_languages_batch.append(detected_lang)

            except Exception as e:
                print(f"⚠️  Translation skipped for batch (error: {str(e)[:50]}...)")
                translated_batch.extend(batch)
            
            # Store results in both translatable_map and memory
            for j in range(len(batch)):
                global_index = batch_idx + j
                token = token_indices[global_index]
                original_text, memory_key = original_texts[token]
                final_text = translated_batch[j]
                detected_lang = detected_languages_batch[j]
                translatable_map[token] = {        
        "text": final_text,        
        "detected_language": detected_lang
                }
 
                
                
                
                if update_memory:
                    translation_memory[memory_key] = final_text
            
            batch_num = batch_idx//batch_size + 1
            total_batches = (len(texts_to_translate) + batch_size - 1)//batch_size
            print(f"✅ Completed batch {batch_num}/{total_batches}")

        print(f"🌐 Translation complete: {translations_added} new translations")

    # Update translation memory if enabled
    if memory_file and update_memory and translation_memory:
        # Ensure directory exists
        memory_dir = os.path.dirname(memory_file)
        if memory_dir:
            os.makedirs(memory_dir, exist_ok=True)
        
        
        with open(memory_file, "w", encoding="utf-8") as f:
            json.dump(translation_memory, f, ensure_ascii=False, indent=2)
        print(f"💾 Updated translation memory: {len(translation_memory)} total entries")

    # Update final metrics
    if metrics:
        metrics["texts_translated"] = translations_added
        
    return translatable_map, translation_memory, original_texts, all_segments_info, metrics
    

def translate_json_file(
    input_file, 
    output_file, 
    target_lang="FR", 
    primary_lang=None, 
    secondary_lang=None, 
    memory_file=None,
    update_memory=False,
    segment_file=None
):
    """Main translation function with enhanced memory support"""
    print(f"🚀 Starting translation: {input_file} -> {target_lang}")

    # Initialize metrics tracking
    deepl_metrics = {
        "total_characters": 0,
        "api_calls": 0,
        "texts_translated": 0,
        "cache_hits": 0,
        "cache_misses": 0
    }
    
    # --- Consolidated directory creation (NEW) ---
    # Output directory (for --output)
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Segment directory (for --segments)
    if segment_file:
        segment_dir = os.path.dirname(segment_file)
        if segment_dir:
            os.makedirs(segment_dir, exist_ok=True)
    
    # Memory directory (for --memory)
    if memory_file:
        memory_dir = os.path.dirname(memory_file)
        if memory_dir:
            os.makedirs(memory_dir, exist_ok=True)
    # --- END directory creation ---

    # Auth check
    auth_key = os.getenv("DEEPL_AUTH_KEY")
    if not auth_key:
        raise ValueError("DEEPL_AUTH_KEY environment variable not set")

    # Initialize translator
    translator = deepl.Translator(auth_key)
    
    # Load input data
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        print(f"📄 Loaded {len(json_data)} blocks from {input_file}")
    except Exception as e:
        raise ValueError(f"Failed to load {input_file}: {e}")

    # Create translation map with improved memory
    translatable_map, translation_memory, original_texts, all_segments_info, deepl_metrics = create_efficient_translatable_map(
        json_data=json_data,
        translator=translator,
        target_lang=target_lang,
        primary_lang=primary_lang,
        secondary_lang=secondary_lang,
        memory_file=memory_file,
        update_memory=update_memory,
        metrics=deepl_metrics
    )

    # Rebuild structure with translations
    translated_data = {}
    for block_id, block_data in json_data.items():
        translated_block = block_data.copy()
        
        if "text" in block_data:
            translation_info = translatable_map.get(block_id)
            if isinstance(translation_info, dict):
                translated_block["text"] = translation_info["text"]
                translated_block["detected_language"] = translation_info["detected_language"]
            else:
                translated_block["text"] = block_data["text"]
  
                
        
        if "segments" in block_data:
            translated_segments = {
                seg_id: translatable_map.get(f"{block_id}_{seg_id}", seg_text)
                for seg_id, seg_text in block_data["segments"].items()
            }
            translated_block["segments"] = translated_segments
        
        translated_data[block_id] = translated_block

    # Save output (now safe since directory exists)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(translated_data, f, indent=2, ensure_ascii=False)
    print(f"✅ Translation completed: {output_file}")
    
    if segment_file:
        segment_translations = {}
        for block_id, block_data in translated_data.items():
            if "segments" in block_data:
                segment_count = len(block_data["segments"])
                if segment_count == 1:
                   # Single segment: use individual translation
                   for seg_id, seg_text in block_data["segments"].items():
                       if isinstance(seg_text, dict) and "text" in seg_text:
                           segment_translations[seg_id] = seg_text["text"]
                       else:
                           segment_translations[seg_id] = seg_text
                elif segment_count > 1:
                   # Multiple segments: split block translation back into segments
                   if "text" in block_data:
                       block_translation = block_data["text"]
                       if isinstance(block_translation, dict):
                           block_translation = block_translation["text"]
                       original_segments = list(block_data["segments"].values())
                       original_lengths = [len(seg.split()) if isinstance(seg, str) else len(seg["text"].split()) for seg in original_segments]
                       # Split the translated block by words and redistribute
                       translated_words = block_translation.split()
                       split_parts = []
                       word_index = 0
                       for length in original_lengths:
                           if word_index + length <= len(translated_words):
                               segment_words = translated_words[word_index:word_index + length]
                               split_parts.append(" ".join(segment_words))
                               word_index += length
                           else:
                               # Fallback: take remaining words
                               remaining_words = translated_words[word_index:]
                               if remaining_words:
                                   split_parts.append(" ".join(remaining_words))
                               break
                       # Assign split parts to segments
                       segment_ids = list(block_data["segments"].keys())
                       for i, seg_id in enumerate(segment_ids):
                           if i < len(split_parts):
                               segment_translations[seg_id] = split_parts[i]
                           else:
                               # Fallback to individual translation if split failed
                               seg_text = block_data["segments"][seg_id]
                               if isinstance(seg_text, dict) and "text" in seg_text:
                                   segment_translations[seg_id] = seg_text["text"]
                               else:
                                   segment_translations[seg_id] = seg_text
    
        with open(segment_file, "w", encoding="utf-8") as f:
            json.dump(segment_translations, f, indent=2, ensure_ascii=False)
        print(f"✅ Segment-only translations exported: {segment_file}")

    # NEW: Track memory usage statistics
    track_memory_usage(
        translation_memory=translation_memory,
        original_texts=original_texts,
        translatable_map=translatable_map,
        output_dir=os.path.dirname(output_file),
        input_filename=input_file,
        all_segments_info=all_segments_info
    )

    # Save DeepL metrics
    metrics_file = os.path.join(os.path.dirname(output_file), "deepl_metrics.json")
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(deepl_metrics, f, indent=2, ensure_ascii=False)
    print(f"📊 DeepL metrics saved: {deepl_metrics}")
    
    return translated_data

def main():
    parser = argparse.ArgumentParser(
        description="Translate JSON content with enhanced global memory support"
    )
    parser.add_argument("--input", "-i", required=True, 
                       help="Input JSON file")
    parser.add_argument("--output", "-o", required=True,
                       help="Output JSON file")
    parser.add_argument("--lang", "-l", required=True,
                       help="Target language code (e.g., FR, ES)")
    parser.add_argument("--primary-lang", 
                       help="Primary source language code")
    parser.add_argument("--secondary-lang",
                       help="Secondary source language code")
    parser.add_argument("--memory", "-m", 
                       help="Path to shared translation memory file")
    parser.add_argument("--update-memory", action="store_true",
                       help="Update translation memory with new translations")
    parser.add_argument("--segments", "-s", 
                       help="Output file for segment-only translations")

    args = parser.parse_args()

    try:
        translate_json_file(
            input_file=args.input,
            output_file=args.output,
            target_lang=args.lang,
            primary_lang=args.primary_lang,
            secondary_lang=args.secondary_lang,
            memory_file=args.memory,
            update_memory=args.update_memory,
            segment_file=args.segments
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
