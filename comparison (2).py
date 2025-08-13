"""
app/comparison.py - Translation comparison functionality
"""

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Any

from flask import (
    Blueprint,
    current_app,
    jsonify,
    render_template,
    request,
    send_from_directory,
    session,
    redirect,
    url_for,
)
from werkzeug.utils import secure_filename

from app.auth import get_current_user

# Create blueprint
bp = Blueprint("comparison", __name__)
logger = logging.getLogger(__name__)

def sanitize_for_log(value: Any, max_length: int = 100) -> str:
    """Sanitize any value for safe logging."""
    if value is None:
        return "None"
    
    str_value = str(value).replace('\n', ' ').replace('\r', ' ')
    
    if len(str_value) > max_length:
        str_value = str_value[:max_length] + "..."
    
    return str_value

def get_session_metadata(session_path: Path) -> dict:
    """Get session metadata, returns empty dict if not found."""
    metadata_file = session_path / "session_metadata.json"
    if metadata_file.exists():
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read session metadata: {e}")
    return {}

@bp.route("/language/<session_id>/<filename>")
def language_comparison(session_id: str, filename: str) -> str:
    """Show block comparison page for a specific file."""
    if not session.get("authenticated"):
        return redirect(url_for("main.login"))
    
    # Sanitize filename to prevent directory traversal
    filename = secure_filename(filename)
    
    original_blocks = {}
    deepl_blocks = {}
    openai_blocks = {}
    translations_metadata = {}
    error_message = None
    
    if hasattr(current_app, "session_manager"):
        try:
            spath = current_app.session_manager.get_session_path(session_id)
            
            # Verify the session belongs to the authenticated user
            if not spath.exists():
                return jsonify(error="Session not found"), 404
            
            # Load original text (with segment format, will be merged in frontend)
            sentences_file = spath / "extracted" / filename / "translatable_flat_sentences.json"
            if sentences_file.exists():
                with open(sentences_file, 'r', encoding='utf-8') as f:
                    sentences_data = json.load(f)
                    
                # Flatten all categories and keep segment format
                for category in sentences_data.values():
                    if isinstance(category, list):  # Ensure it's a list
                        for block_info in category:
                            if isinstance(block_info, dict):  # Ensure it's a dict
                                for block_id, text in block_info.items():
                                    if block_id != "tag":  # Skip tag fields
                                        # Handle special case where block IDs are combined
                                        if "=" in block_id:
                                            # Split and add each ID separately
                                            ids = block_id.split("=")
                                            for single_id in ids:
                                                single_id = single_id.strip()
                                                if single_id:  # Ensure not empty
                                                    original_blocks[single_id] = text
                                        else:
                                            original_blocks[block_id] = text
            
            # Load DeepL translations (with segment format, will be merged in frontend)
            deepl_file = spath / "translated" / filename / "segments_only.json"
            if deepl_file.exists():
                with open(deepl_file, 'r', encoding='utf-8') as f:
                    deepl_data = json.load(f)
                    # Ensure it's a dictionary before assigning
                    if isinstance(deepl_data, dict):
                        deepl_blocks = deepl_data
            
            # Load OpenAI translations (with segment format, will be merged in frontend)
            openai_file = spath / "refined" / filename / "openai_translations.json"
            if openai_file.exists():
                with open(openai_file, 'r', encoding='utf-8') as f:
                    openai_data = json.load(f)
                    # Ensure it's a dictionary before assigning
                    if isinstance(openai_data, dict):
                        openai_blocks = openai_data
            translations_file = spath / "translated" / filename / "translations.json"
            if translations_file.exists():
                with open(translations_file, 'r', encoding='utf-8') as f:
                    translations_data = json.load(f)
                    # Extract metadata for each block
                    if isinstance(translations_data, dict):
                        for block_id, block_info in translations_data.items():
                            if isinstance(block_info, dict):
                                metadata = {}
                                if block_info.get('reordered'):
                                    metadata['reordered'] = True
                                if 'word_count_change' in block_info and 'change_percent' in block_info['word_count_change']:
                                    metadata['change_percent'] = block_info['word_count_change']['change_percent']
                                if metadata:  # Only add if there's actual metadata
                                    translations_metadata[block_id] = metadata
                                    

        
                    
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}: {e}")
            error_message = f"Error reading JSON data: Invalid format"
        except Exception as e:
            logger.error(f"Error loading translation data for {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}: {e}")
            error_message = f"Error loading translation data: {str(e)}"
    else:
        error_message = "Session manager not available"
    
    return render_template(
        "language_comparison.html",
        session_id=session_id,
        filename=filename,
        original_blocks=original_blocks,
        deepl_blocks=deepl_blocks,
        openai_blocks=openai_blocks,
        translations_metadata=translations_metadata,
        error_message=error_message,
        current_user=get_current_user(),
    )

@bp.route("/api/available-files/<session_id>")
def api_available_files(session_id: str):
    """Get available files for translation comparison"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    available_files = []
    
    if hasattr(current_app, "session_manager"):
        try:
            spath = current_app.session_manager.get_session_path(session_id)
            uploads_dir = spath / "uploads"
           
            extracted_dir = spath / "extracted"
            
            if uploads_dir.exists() and extracted_dir.exists():
                for uploaded_file in uploads_dir.iterdir():
                         if uploaded_file.is_file() and uploaded_file.suffix in ['.html', '.sql']:
                             folder_name = uploaded_file.stem
                             extracted_folder = extracted_dir / folder_name
                             sentences_file = extracted_folder / "translatable_flat_sentences.json"
                             if sentences_file.exists():
                                 available_files.append(uploaded_file.name)
            
            
            return jsonify(success=True, files=sorted(available_files))
                            
        except Exception as e:
            logger.error(f"Error loading files for session {sanitize_for_log(session_id)}: {e}")
            return jsonify(success=False, error=str(e)), 500
    
    return jsonify(success=False, error="Session manager not available"), 500

@bp.route("/api/regenerate/<session_id>", methods=["POST"])
def api_regenerate(session_id: str):
    """Regenerate final HTML file with user's edited translations"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    if not hasattr(current_app, "session_manager"):
        return jsonify(error="Session manager not available"), 500
    
    try:
        # Get session path
        spath = current_app.session_manager.get_session_path(session_id)
        
        # Get form data
        translation_type = request.form.get("translation_type")  # 'deepl' or 'openai'
        updated_blocks_json = request.form.get("updated_blocks")
        
        if not translation_type or not updated_blocks_json:
            return jsonify(error="Missing required parameters"), 400
        
        if translation_type not in ['deepl', 'openai']:
            return jsonify(error="Invalid translation type"), 400
        
        # Parse updated blocks
        try:
            updated_blocks = json.loads(updated_blocks_json)
        except json.JSONDecodeError:
            return jsonify(error="Invalid JSON format"), 400
        
        logger.info(f"[Regenerate] Processing {sanitize_for_log(translation_type)} for session {sanitize_for_log(session_id)}")
        logger.info(f"[Regenerate] Updated blocks: {list(updated_blocks.keys())}")
        
        # Find the HTML file (should be only one for text sessions)
        uploads_dir = spath / "uploads"
        uploaded_file = uploads_dir / filename
        if not uploaded_file.exists():
            return jsonify(error="Uploaded file not found"), 404
        
        
        # Use files
        basename = uploaded_file.stem
        
        # Determine which translation file to update
        if translation_type == 'deepl':
            segments_file = spath / "translated" / basename / "segments_only.json"
        else:  # openai
            segments_file = spath / "refined" / basename / "openai_translations.json"
        
        if not segments_file.exists():
            return jsonify(error=f"Original {translation_type} translation file not found"), 404
        
        # Load original translation file
        with open(segments_file, 'r', encoding='utf-8') as f:
            original_segments = json.load(f)
        
        logger.info(f"[Regenerate] Original segments: {len(original_segments)}")
        
        # Update segments with user's edits
        updated_segments = original_segments.copy()
        
        # Get all block numbers that were edited
        edited_block_numbers = set()
        for block_key in updated_blocks.keys():
            match = re.match(r'BLOCK_(\d+)', block_key)
            if match:
                edited_block_numbers.add(int(match.group(1)))
        
        # Remove all segments for edited blocks
        for block_num in edited_block_numbers:
            keys_to_remove = [key for key in updated_segments.keys() 
                            if key.startswith(f"BLOCK_{block_num}_")]
            for key in keys_to_remove:
                del updated_segments[key]
        
        # Add the new edited segments
        for block_key, edited_text in updated_blocks.items():
            clean_text = edited_text
            MAX_TEXT_LENGTH = 100000
            if len(clean_text) > MAX_TEXT_LENGTH:
                logger.warning(f"[Regenerate] Text for {sanitize_for_log(block_key)} truncated from {len(clean_text)} to {MAX_TEXT_LENGTH} characters")
                clean_text = clean_text[:MAX_TEXT_LENGTH]
            
            # Remove block identifiers
            while True:
                match = re.search(r'BLOCK_\d+(?:_S\d+)?', clean_text)
                if not match:
                    break
                start = match.start()
                end = match.end()
                while start > 0 and clean_text[start-1] in ' \t\n\r':
                    start -= 1
                while end < len(clean_text) and clean_text[end] in ' \t\n\r':
                    end += 1
                clean_text = clean_text[:start] + ' ' + clean_text[end:]
            clean_text = ' '.join(clean_text.split())    
            if clean_text:
                updated_segments[block_key] = clean_text
                logger.info(f"[Regenerate] Added clean segment {sanitize_for_log(block_key)}: {sanitize_for_log(clean_text,50)}")
        
        logger.info(f"[Regenerate] Updated segments: {len(updated_segments)}")
        
        # Save updated segments to a temporary file
        temp_segments_file = segments_file.parent / f"segments_edited_{int(time.time())}.json"
        with open(temp_segments_file, 'w', encoding='utf-8') as f:
            json.dump(updated_segments, f, indent=2, ensure_ascii=False)

        # Get session metadata to determine target language
        metadata = get_session_metadata(spath)
        target_lang = "fr"  # Default fallback
        
        # Prepare paths for merging
        file_extension = Path(filename).suffix
        template_name = f"non_translatable{file_extension}"
        non_translatable_template = spath / "extracted" / basename / template_name
        output_dir = spath / "results"
        output_dir.mkdir(exist_ok=True)
        
        if not non_translatable_template.exists():
            return jsonify(error="Non-translatable HTML template not found"), 404
        
        # Use existing step4_merge.py logic
        script_path = Path(__file__).parent.parent / "core_scripts" / "step4_merge.py"
        
        logger.info(f"[Regenerate] Looking for script at: {sanitize_for_log(script_path)}")
        
        if not script_path.exists():
            return jsonify(error="Merge script not found"), 500
        
        # Build merge command
        if translation_type == 'deepl':
            temp_output_file = output_dir / f"temp_final_deepl_edited.html"
            cmd = ["python3", str(script_path)]
            cmd.extend([
                "--html", str(non_translatable_template),
                "--deepl", str(temp_segments_file),
                "--output-deepl", str(temp_output_file),
                "--target-lang", target_lang.lower()
            ])
        else:
            original_deepl_file = spath / "translated" / basename / "segments_only.json"
            if not original_deepl_file.exists():
                return jsonify(error="Original DeepL translation file required for OpenAI merge"), 404
            
            temp_output_file = output_dir / f"temp_final_openai_edited.html"
            temp_deepl_output = output_dir / f"temp_final_deepl_unused.html"
            cmd = ["python3", str(script_path)]
            cmd.extend([
                "--html", str(non_translatable_template),
                "--deepl", str(original_deepl_file),
                "--openai", str(temp_segments_file),
                "--output-deepl", str(temp_deepl_output),
                "--output-openai", str(temp_output_file),
                "--target-lang", target_lang.lower()
            ])
        
        logger.info(f"[Regenerate] Merge command: {' '.join(cmd)}")
        
        # Execute merge command
        import subprocess
        try:
            result = subprocess.run(
                cmd,
                cwd=spath,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info(f"[Regenerate] Merge completed successfully")
                logger.info(f"[Regenerate] Stdout: {result.stdout}")
                
                # Clean up temp file
                if temp_segments_file.exists():
                    temp_segments_file.unlink()
                
                # Search for the actual generated file
                actual_output_patterns = [
                    f"final_{translation_type}_edited-{target_lang.lower()}.html",
                    f"final_{translation_type}_edited.html",
                    f"temp_final_{translation_type}_edited-{target_lang.lower()}.html",
                    f"temp_final_{translation_type}_edited.html",
                    f"final_{translation_type}-{target_lang.lower()}.html",
                    f"final_{translation_type}.html"
                ]
                
                actual_output_file = None
                for pattern in actual_output_patterns:
                    potential_file = output_dir / pattern
                    if potential_file.exists():
                        actual_output_file = potential_file
                        break
                
                # If specific patterns don't work, search for recent HTML files
                if not actual_output_file:
                    for html_file in output_dir.glob("*.html"):
                        if time.time() - html_file.stat().st_mtime < 60:
                            actual_output_file = html_file
                            break
                
                if actual_output_file and actual_output_file.exists():
                    return send_from_directory(
                        actual_output_file.parent,
                        actual_output_file.name,
                        as_attachment=True,
                        download_name=f"final_{translation_type}_edited.html"
                    )
                else:
                    logger.error(f"[Regenerate] No output file found in {sanitize_for_log(output_dir)}")
                    return jsonify(error="Output file was not created"), 500
                    
            else:
                logger.error(f"[Regenerate] Merge failed (exit code {result.returncode})")
                logger.error(f"[Regenerate] Stderr: {result.stderr}")
                logger.error(f"[Regenerate] Stdout: {result.stdout}")
                
                if temp_segments_file.exists():
                    temp_segments_file.unlink()
                return jsonify(error=f"Merge process failed: {result.stderr}"), 500
                
        except subprocess.TimeoutExpired:
            logger.error(f"[Regenerate] Merge process timed out")
            if temp_segments_file.exists():
                temp_segments_file.unlink()
            return jsonify(error="Merge process timed out"), 500
        except Exception as e:
            logger.error(f"[Regenerate] Subprocess error: {e}")
            if temp_segments_file.exists():
                temp_segments_file.unlink()
            return jsonify(error=f"Process execution error: {str(e)}"), 500
        
    except Exception as e:
        logger.error(f"[Regenerate] Error: {e}")
        logger.exception("Full traceback:")
        return jsonify(error=f"Server error: {str(e)}"), 500

@bp.route("/api/edits/<session_id>/<filename>", methods=["GET"])
def api_get_edits(session_id: str, filename: str):
    """Load all existing edits for a specific file"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    if not hasattr(current_app, "session_manager"):
        return jsonify(error="Session manager not available"), 500
    
    try:
        # Sanitize filename
        filename = secure_filename(filename)
        
        # Get session path
        spath = current_app.session_manager.get_session_path(session_id)
        if not spath.exists():
            return jsonify(error="Session not found"), 404
        
        # Create edits directory if it doesn't exist
        edits_dir = spath / "edits"
        edits_dir.mkdir(exist_ok=True)
        
        # Load all edit files
        edits = {
            "deepl_block_edits": {},
            "openai_block_edits": {},
            "deepl_full_edits": {},
            "openai_full_edits": {}
        }
        
        # Define edit file mappings
        edit_files = {
            "deepl_block_edits": f"{filename}_deepl_block_edits.json",
            "openai_block_edits": f"{filename}_openai_block_edits.json",
            "deepl_full_edits": f"{filename}_deepl_full_edits.json",
            "openai_full_edits": f"{filename}_openai_full_edits.json"
        }
        
        # Load each edit file if it exists
        for edit_type, edit_filename in edit_files.items():
            edit_file = edits_dir / edit_filename
            if edit_file.exists():
                try:
                    with open(edit_file, 'r', encoding='utf-8') as f:
                        edits[edit_type] = json.load(f)
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Could not load {edit_filename}: {e}")
                    edits[edit_type] = {}
        
        logger.info(f"[Load Edits] Loaded edits for {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}")
        
        return jsonify(success=True, edits=edits)
        
    except Exception as e:
        logger.error(f"[Load Edits] Error: {e}")
        return jsonify(error=str(e)), 500


@bp.route("/api/edits/<session_id>/<filename>", methods=["POST"])
def api_save_edit(session_id: str, filename: str):
    """Save a single edit (block or full text)"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    if not hasattr(current_app, "session_manager"):
        return jsonify(error="Session manager not available"), 500
    
    try:
        # Sanitize filename
        filename = secure_filename(filename)
        
        # Get session path
        spath = current_app.session_manager.get_session_path(session_id)
        if not spath.exists():
            return jsonify(error="Session not found"), 404
        
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify(error="No JSON data provided"), 400
        
        edit_type = data.get("edit_type")  # "deepl_block", "openai_block", "deepl_full", "openai_full"
        block_id = data.get("block_id")    # For block edits, "complete" for full edits
        text = data.get("text")
        
        if not all([edit_type, block_id, text]):
            return jsonify(error="Missing required fields"), 400
        
        if edit_type not in ["deepl_block", "openai_block", "deepl_full", "openai_full"]:
            return jsonify(error="Invalid edit type"), 400
        
        # Create edits directory
        edits_dir = spath / "edits"
        edits_dir.mkdir(exist_ok=True)
        
        # Determine edit file
        edit_filename = f"{filename}_{edit_type}_edits.json"
        edit_file = edits_dir / edit_filename
        
        # Load existing edits
        existing_edits = {}
        if edit_file.exists():
            try:
                with open(edit_file, 'r', encoding='utf-8') as f:
                    existing_edits = json.load(f)
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Could not load existing edits from {edit_filename}: {e}")
                existing_edits = {}
        
        # Add/update the edit
        existing_edits[block_id] = text
        
        # Save back to file
        with open(edit_file, 'w', encoding='utf-8') as f:
            json.dump(existing_edits, f, indent=2, ensure_ascii=False)
        
        logger.info(f"[Save Edit] Saved {sanitize_for_log(edit_type)} edit for {sanitize_for_log(block_id)} in {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}")
        
        return jsonify(success=True, message="Edit saved successfully")
        
    except Exception as e:
        logger.error(f"[Save Edit] Error: {e}")
        return jsonify(error=str(e)), 500


@bp.route("/api/edits/<session_id>/<filename>/<edit_type>/<block_id>", methods=["DELETE"])
def api_delete_edit(session_id: str, filename: str, edit_type: str, block_id: str):
    """Delete a specific edit"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    if not hasattr(current_app, "session_manager"):
        return jsonify(error="Session manager not available"), 500
    
    try:
        # Sanitize inputs
        filename = secure_filename(filename)
        
        if edit_type not in ["deepl_block", "openai_block", "deepl_full", "openai_full"]:
            return jsonify(error="Invalid edit type"), 400
        
        # Get session path
        spath = current_app.session_manager.get_session_path(session_id)
        if not spath.exists():
            return jsonify(error="Session not found"), 404
        
        # Get edit file
        edits_dir = spath / "edits"
        edit_filename = f"{filename}_{edit_type}_edits.json"
        edit_file = edits_dir / edit_filename
        
        if not edit_file.exists():
            return jsonify(success=True, message="Edit file does not exist")
        
        # Load existing edits
        try:
            with open(edit_file, 'r', encoding='utf-8') as f:
                existing_edits = json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Could not load edits from {edit_filename}: {e}")
            return jsonify(error="Could not load edit file"), 500
        
        # Remove the specific edit
        if block_id in existing_edits:
            del existing_edits[block_id]
            
            # Save back to file (or delete file if empty)
            if existing_edits:
                with open(edit_file, 'w', encoding='utf-8') as f:
                    json.dump(existing_edits, f, indent=2, ensure_ascii=False)
            else:
                # Delete the file if no edits remain
                edit_file.unlink()
            
            logger.info(f"[Delete Edit] Removed {sanitize_for_log(edit_type)} edit for {sanitize_for_log(block_id)} in {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}")
        
        return jsonify(success=True, message="Edit deleted successfully")
        
    except Exception as e:
        logger.error(f"[Delete Edit] Error: {e}")
        return jsonify(error=str(e)), 500


@bp.route("/api/edits/<session_id>/<filename>/<edit_type>", methods=["DELETE"])
def api_clear_all_edits(session_id: str, filename: str, edit_type: str):
    """Clear all edits for a specific type (deepl_block, openai_block, deepl_full, openai_full)"""
    if not session.get("authenticated"):
        return jsonify(error="Authentication required"), 401
    
    if not hasattr(current_app, "session_manager"):
        return jsonify(error="Session manager not available"), 500
    
    try:
        # Sanitize inputs
        filename = secure_filename(filename)
        
        if edit_type not in ["deepl_block", "openai_block", "deepl_full", "openai_full"]:
            return jsonify(error="Invalid edit type"), 400
        
        # Get session path
        spath = current_app.session_manager.get_session_path(session_id)
        if not spath.exists():
            return jsonify(error="Session not found"), 404
        
        # Get edit file
        edits_dir = spath / "edits"
        edit_filename = f"{filename}_{edit_type}_edits.json"
        edit_file = edits_dir / edit_filename
        
        # Delete the file if it exists
        if edit_file.exists():
            edit_file.unlink()
            logger.info(f"[Clear All Edits] Cleared all {sanitize_for_log(edit_type)} edits in {sanitize_for_log(session_id)}/{sanitize_for_log(filename)}")
        
        return jsonify(success=True, message=f"All {edit_type} edits cleared successfully")
        
    except Exception as e:
        logger.error(f"[Clear All Edits] Error: {e}")
        return jsonify(error=str(e)), 500