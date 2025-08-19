import subprocess
import logging
from pathlib import Path
import os
import zipfile
import time
import json
from flask import current_app


class PipelineRunner:
    def __init__(self, session_path, env):
        """
        session_path: a pathlib.Path pointing to the <session_id>/ directory
        env: os.environ (or a copy of it) to pass through to subprocesses
        """
        self.session_path = Path(session_path)
        self.env = env
        self.logger = logging.getLogger('pipeline')
        self.session_id = self.session_path.name
        self.tracker = current_app.progress_tracker

        # ADD THE NEW LOGGING LINES RIGHT HERE:
        self.logger.info(f"[Pipeline] Session path: {self.session_path}")
        self.logger.info(f"[Pipeline] Session path exists: {self.session_path.exists()}")
        self.logger.info(f"[Pipeline] Session path is absolute: {self.session_path.is_absolute()}")
        self.logger.info(f"[Pipeline] Current working directory: {os.getcwd()}")
    
        
        # Track processing results
        self.results = {
            'files_processed': 0,
            'files_extracted': 0,
            'files_translated': 0,
            'files_refined': 0,
            'files_merged': 0,
            'errors': []
        }

    
    def _generate_memory_report(self):
        """Generate a comprehensive memory usage report after processing"""
        
        try:
            # Check for batch memory stats
            batch_stats_file = self.session_path / "memory_usage_batch.json"
            
            if batch_stats_file.exists():
                with open(batch_stats_file, 'r') as f:
                    batch_data = json.load(f)
                
                # Create formatted report
                report_lines = [
                    "TRANSLATION MEMORY EFFICIENCY REPORT",
                    "=" * 60,
                    f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                    f"Session: {self.session_id}",
                    "",
                    "📊 PROCESSING SUMMARY:",
                    f"  Files processed: {batch_data['summary']['total_files_processed']}",
                    f"  Total segments: {batch_data['summary']['total_segments_processed']:,}",
                    f"  Cache hits: {batch_data['summary']['total_cache_hits']:,}",
                    f"  Cache misses: {batch_data['summary']['total_cache_misses']:,}",
                    f"  Overall hit rate: {batch_data['summary']['overall_hit_rate_percent']}%",
                    f"  Final memory size: {batch_data['summary']['final_memory_size']:,} entries",
                    "",
                    "📋 FILE-BY-FILE BREAKDOWN:",
                    "-" * 60
                ]
                
                for i, stats in enumerate(batch_data['cumulative_stats'], 1):
                    hit_rate = stats['hit_rate_percent']
                    if hit_rate > 70:
                        efficiency = "🔥 Excellent"
                    elif hit_rate > 40:
                        efficiency = "✅ Good"
                    elif hit_rate > 10:
                        efficiency = "⚠️ Moderate"
                    else:
                        efficiency = "🆕 Learning"
                    
                    file_name = Path(stats['file']).stem
                    report_lines.extend([
                        f"{i:2d}. {efficiency} - {file_name}",
                        f"    Hit rate: {hit_rate}%",
                        f"    Segments: {stats['total_segments']:,} | Hits: {stats['cache_hits']:,} | New: {stats['new_entries_added']:,}",
                        ""
                    ])
                
                # Add efficiency insights
                overall_rate = batch_data['summary']['overall_hit_rate_percent']
                if overall_rate > 50:
                    insight = "🎉 Excellent efficiency! Your files share significant common content."
                elif overall_rate > 30:
                    insight = "✅ Good efficiency. The translation memory provides substantial value."
                elif overall_rate > 15:
                    insight = "⚠️ Moderate efficiency. Files have some overlapping content."
                else:
                    insight = "📝 Low hit rate. Files appear to have mostly unique content."
                
                # Calculate API savings
                total_hits = batch_data['summary']['total_cache_hits']
                api_calls_saved = total_hits // 330  # Assuming 330 segments per API call
                estimated_cost_savings = api_calls_saved * 0.002  # Rough DeepL cost estimate
                
                report_lines.extend([
                    "💡 EFFICIENCY ANALYSIS:",
                    "-" * 60,
                    insight,
                    "",
                    f"🔄 Memory grew from 0 to {batch_data['summary']['final_memory_size']:,} entries",
                    f"💰 Estimated API calls saved: ~{api_calls_saved}",
                    f"💰 Estimated cost savings: ~${estimated_cost_savings:.3f}",
                    "",
                    "📈 PROCESSING PATTERN:",
                    "-" * 60
                ])
                
                # Show memory growth pattern
                cumulative_memory = 0
                for i, stats in enumerate(batch_data['cumulative_stats']):
                    cumulative_memory += stats['new_entries_added']
                    report_lines.append(
                        f"After file {i+1:2d}: {cumulative_memory:,} total entries "
                        f"(+{stats['new_entries_added']:,} new)"
                    )
                
                report_lines.extend([
                    "",
                    "=" * 60,
                    "💡 TIP: Higher hit rates indicate more similar content between files.",
                    "    Consider grouping similar files for maximum efficiency!"
                ])
                
                # Save report
                report_file = self.session_path / "results" / "memory_efficiency_report.txt"
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(report_lines))
                
                self.logger.info(f"[Pipeline] Generated memory efficiency report: results/memory_efficiency_report.txt")
                
                # Also log key stats to console
                self.logger.info(f"[Pipeline] 📊 Memory efficiency: {overall_rate}% hit rate, "
                               f"{batch_data['summary']['total_cache_hits']:,}/{batch_data['summary']['total_segments_processed']:,} segments cached")
                
                # Save machine-readable stats for potential API consumption
                stats_summary = {
                    'session_id': self.session_id,
                    'timestamp': time.time(),
                    'summary': batch_data['summary'],
                    'file_count': len(batch_data['cumulative_stats']),
                    'efficiency_rating': 'excellent' if overall_rate > 50 else 'good' if overall_rate > 30 else 'moderate' if overall_rate > 15 else 'low'
                }
                
                summary_file = self.session_path / "results" / "memory_stats_summary.json"
                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(stats_summary, f, indent=2)
                
                return True
                
            else:
                self.logger.info("[Pipeline] No memory statistics available - memory tracking may not be enabled")
                return False
                
        except Exception as e:
            self.logger.error(f"[Pipeline] Failed to generate memory report: {e}")
            self.logger.exception("Full traceback:")
            return False   
    
    def _generate_consolidated_metrics_report(self, html_filenames, enable_refinement=True):
        """Generate a consolidated metrics report combining DeepL and OpenAI usage"""
        try:
            consolidated_metrics = {
                'session_id': self.session_id,
                'total_files': len(html_filenames),
                'refinement_enabled': enable_refinement,
                'deepl_total': {
                    'total_characters': 0,
                    'api_calls': 0,
                    'texts_translated': 0,
                    'total_cache_hits': 0,
                    'total_cache_misses': 0
                },
                'openai_total': {
                    'total_tokens': 0,
                    'prompt_tokens': 0,
                    'completion_tokens': 0,
                    'api_calls': 0,
                    'blocks_processed': 0,
                    'blocks_improved': 0
                },
                'file_details': []
            }
            
            # Collect DeepL metrics from each file
            for html_name in html_filenames:
                basename = Path(html_name).stem
                file_metrics = {'filename': html_name}
                
                # DeepL metrics
                deepl_file = self.session_path / "translated" / basename / "deepl_metrics.json"
                if deepl_file.exists():
                    with open(deepl_file, 'r') as f:
                        deepl_data = json.load(f)
                        file_metrics['deepl'] = deepl_data
                        
                        # Add to totals
                        for key in ['total_characters', 'api_calls', 'texts_translated', 'cache_hits', 'cache_misses']:
                            if key in deepl_data:
                                total_key = f"total_{key}" if key in ['cache_hits', 'cache_misses'] else key
                                consolidated_metrics['deepl_total'][total_key] += deepl_data[key]
                
                # OpenAI metrics (if refinement enabled)
                if enable_refinement:
                    openai_file = self.session_path / "refined" / basename / "openai_metrics.json"
                    if openai_file.exists():
                        with open(openai_file, 'r') as f:
                            openai_data = json.load(f)
                            file_metrics['openai'] = openai_data
                            
                            # Add to totals
                            for key in ['total_tokens', 'prompt_tokens', 'completion_tokens', 'api_calls', 'blocks_processed', 'blocks_improved']:
                                if key in openai_data:
                                    consolidated_metrics['openai_total'][key] += openai_data[key]
                
                consolidated_metrics['file_details'].append(file_metrics)
            
            # Calculate estimated costs with billing formulas
            deepl_chars = consolidated_metrics['deepl_total']['total_characters']
            openai_tokens = consolidated_metrics['openai_total']['total_tokens']
            deepl_texts = consolidated_metrics['deepl_total']['texts_translated']
            deepl_api_calls = consolidated_metrics['deepl_total']['api_calls']
            openai_prompt_tokens = consolidated_metrics['openai_total']['prompt_tokens']
            openai_completion_tokens = consolidated_metrics['openai_total']['completion_tokens']
            openai_api_calls = consolidated_metrics['openai_total']['api_calls']
            
            # DeepL pricing (per text translated, not per character)
            deepl_price_per_text = 0.00076  # Placeholder price per text
            deepl_cost = deepl_texts * deepl_price_per_text
            
            # OpenAI pricing (GPT-4 approximate)
            openai_input_price = 0.000003   # $0.003 per 1000 input tokens  
            openai_output_price = 0.000015  # $0.015 per 1000 output tokens
            openai_cost = (openai_prompt_tokens * openai_input_price + openai_completion_tokens * openai_output_price)
            
            # Calculate per-file costs
            per_file_billing = []
            for file_detail in consolidated_metrics['file_details']:
                filename = file_detail["filename"]
                deepl_data = file_detail.get("deepl", {})
                openai_data = file_detail.get("openai", {})
                
                # Calculate individual file costs
                file_deepl_cost = deepl_data.get("texts_translated", 0) * deepl_price_per_text
                file_openai_cost = (openai_data.get("prompt_tokens", 0) * openai_input_price + 
                                   openai_data.get("completion_tokens", 0) * openai_output_price)
                
                per_file_billing.append({
                    "filename": filename,
                    "deepl_usd": round(file_deepl_cost, 4),
                    "deepl_formula": f"texts_translated({deepl_data.get('texts_translated', 0)}) * ${deepl_price_per_text}_per_text",
                    "openai_usd": round(file_openai_cost, 4),
                    "openai_formula": f"prompt_tokens({openai_data.get('prompt_tokens', 0)}) * ${openai_input_price} + completion_tokens({openai_data.get('completion_tokens', 0)}) * ${openai_output_price}"
                })
            
            consolidated_metrics['estimated_costs'] = {
                'deepl_usd': round(deepl_cost, 4),
                'deepl_formula': f"texts_translated({deepl_texts}) * ${deepl_price_per_text}_per_text",
                'openai_usd': round(openai_cost, 4),
                'openai_formula': f"prompt_tokens({openai_prompt_tokens}) * ${openai_input_price} + completion_tokens({openai_completion_tokens}) * ${openai_output_price}",
                'total_usd': round(deepl_cost + openai_cost, 4),
                'billing_breakdown': {
                    'deepl_texts_billed': deepl_texts,
                    'deepl_api_calls': deepl_api_calls,
                    'openai_input_tokens': openai_prompt_tokens,
                    'openai_output_tokens': openai_completion_tokens,
                    'openai_api_calls': openai_api_calls
                },
                'per_file_billing': per_file_billing
            }
            
            # Save consolidated report
            report_file = self.session_path / "refined" / "consolidated_metrics.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(consolidated_metrics, f, indent=2, ensure_ascii=False)
            
            # Create human-readable summary
            summary_lines = [
                "TRANSLATION METRICS SUMMARY",
                "=" * 50,
                f"Session: {self.session_id}",
                f"Files processed: {len(html_filenames)}",
                "",
                "🌐 DEEPL TRANSLATION:",
                f"  Characters translated: {deepl_chars:,}",
                f"  API calls made: {consolidated_metrics['deepl_total']['api_calls']}",
                f"  Cache efficiency: {consolidated_metrics['deepl_total']['total_cache_hits']} hits / {consolidated_metrics['deepl_total']['total_cache_misses']} misses",
                "",
                "🤖 OPENAI REFINEMENT:" if enable_refinement else "🤖 OPENAI REFINEMENT: (Disabled)",
            ]
            
            if enable_refinement and openai_tokens > 0:
                summary_lines.extend([
                    f"  Total tokens: {openai_tokens:,}",
                    f"  Prompt tokens: {consolidated_metrics['openai_total']['prompt_tokens']:,}",
                    f"  Completion tokens: {consolidated_metrics['openai_total']['completion_tokens']:,}",
                    f"  API calls made: {consolidated_metrics['openai_total']['api_calls']}",
                    f"  Blocks improved: {consolidated_metrics['openai_total']['blocks_improved']}",
                ])
            else:
                summary_lines.append("  No refinement data available")
            
            summary_lines.extend([
                "",
                "💰 ESTIMATED COSTS:",
                f"  DeepL: ~${consolidated_metrics['estimated_costs']['deepl_usd']:.4f}",
                f"  OpenAI: ~${consolidated_metrics['estimated_costs']['openai_usd']:.4f}",
                f"  Total: ~${consolidated_metrics['estimated_costs']['total_usd']:.4f}",
                "",
                "=" * 50
            ])
            
            # Save readable summary
            summary_file = self.session_path / "results" / "metrics_summary.txt"
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(summary_lines))
            
            self.logger.info(f"[Pipeline] Consolidated metrics saved: {consolidated_metrics['estimated_costs']['total_usd']:.4f} USD estimated cost")
            
            return True
            
        except Exception as e:
            self.logger.error(f"[Pipeline] Failed to generate consolidated metrics: {e}")
            return False   
    
    def _execute_script(self, script_name, args):
        """
        Run a Python script under core_scripts/ with the given arguments.
        Return True on exit code 0, otherwise False.
        """
        if script_name == "step1_extract.py":
           script_path = (Path(__file__).parent.parent.parent/ "core_scripts"/ "extraction"/ script_name
           )
        else:
        # All other scripts (step2, step3, step4) remain in core_scripts root
           script_path = (
            Path(__file__).parent.parent.parent
            / "core_scripts"
            / script_name
        )
        
        self.logger.info(f"[Pipeline] Looking for script at: {script_path}")
        
        if not script_path.exists():
            self.logger.error(f"[Pipeline] Script not found: {script_name} at {script_path}")
            self.tracker.update(
                self.session_id, 
                1, 
                f"Error: Script {script_name} not found"
            )
            return False

        cmd = ["python3", str(script_path)] + args
        self.logger.info(f"[Pipeline] Executing: {' '.join(cmd)}")
        
        # Update progress before starting the script
        step_names = {
            "step1_extract.py": (1, "Extracting content from HTML..."),
            "step2_translate.py": (2, "Translating content..."),
            "step3_gpt_process.py": (3, "Refining with GPT..."),
            "step4_merge.py": (4, "Merging into final HTML files...")
        }
        
        if script_name in step_names:
            step_num, step_msg = step_names[script_name]
            self.tracker.update(self.session_id, step_num, step_msg)
        
        try:
            # Create subprocess with proper environment
            process = subprocess.Popen(
                cmd,
                cwd=self.session_path,
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Monitor the process with periodic progress updates
            start_time = time.time()
            timeout = 300  # 5 minutes per script
            check_interval = 5  # Check every 5 seconds
            
            while True:
                # Check if process is still running
                poll_result = process.poll()
                
                if poll_result is not None:
                    # Process finished
                    stdout, stderr = process.communicate()
                    
                    if poll_result == 0:
                        self.logger.info(f"[Pipeline] {script_name} completed successfully")
                        if stdout:
                            self.logger.debug(f"[Pipeline] {script_name} output: {stdout[:500]}")  # First 500 chars
                        return True
                    else:
                        self.logger.error(f"[Pipeline] {script_name} failed (exit code {poll_result})")
                        if stderr:
                            self.logger.error(f"[Pipeline] stderr: {stderr}")
                            # Also add to results errors for user visibility
                            self.results['errors'].append(f"{script_name}: {stderr[:200]}")
                        if stdout:
                            self.logger.error(f"[Pipeline] stdout: {stdout}")
                        
                        # Update tracker with specific error
                        error_msg = stderr.strip() if stderr else f"Exit code {poll_result}"
                        if script_name in step_names:
                            step_num, _ = step_names[script_name]
                            self.tracker.update(
                                self.session_id, 
                                step_num, 
                                f"Error: {error_msg[:100]}",
                                error_code="script_failed"
                            )
                        return False
                
                # Check for timeout
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    self.logger.error(f"[Pipeline] {script_name} timed out after {elapsed:.1f} seconds")
                    process.terminate()
                    time.sleep(1)
                    if process.poll() is None:
                        process.kill()
                    self.tracker.update(
                        self.session_id, 
                        step_num if script_name in step_names else 1, 
                        f"Error: {script_name} timed out"
                    )
                    return False
                
                # Update progress with elapsed time
                if script_name in step_names and int(elapsed) % 10 == 0:
                    step_num, base_msg = step_names[script_name]
                    self.tracker.update(
                        self.session_id, 
                        step_num, 
                        f"{base_msg} ({int(elapsed)}s)"
                    )
                
                # Wait before next check
                time.sleep(check_interval)
                
        except Exception as e:
            self.logger.error(f"[Pipeline] {script_name} execution error: {str(e)}")
            self.logger.exception("Full traceback:")
            self.tracker.update(
                self.session_id, 
                1, 
                f"Error executing {script_name}: {str(e)}"
            )
            return False

    def _create_per_file_zip(self, basename, results_base, final_base):
        """Create a ZIP for one file (final outputs only)."""
        zip_path = results_base / f"{basename}.zip"
        try:
            if zip_path.exists():
                zip_path.unlink()
                
            with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
                final_dir = final_base / basename
                if final_dir.exists():
                    for file_path in final_dir.rglob('*'):
                        if file_path.is_file():
                            rel_path = file_path.relative_to(final_dir)
                            archive.write(file_path, arcname=str(rel_path))
                            
            self.logger.info(f"[Pipeline] Created per-file ZIP: results/{basename}.zip")
            return True
        except Exception as e:
            self.logger.error(f"[Pipeline] Failed to create ZIP for {basename}: {e}")
            self.results['errors'].append(f"ZIP creation failed for {basename}: {str(e)}")
            return False

    def _create_batch_output_zip(self, results_base):
        """Create comprehensive batch output ZIP with all intermediate files."""
        zip_path = results_base / "batch-output.zip"
        try:
            if zip_path.exists():
                zip_path.unlink()
                
            with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
                # Add all step directories
                for step_dir in ['extracted', 'translated', 'refined', 'final']:
                    step_path = self.session_path / step_dir
                    if step_path.exists():
                        for file_path in step_path.rglob('*'):
                            if file_path.is_file():
                                rel_path = file_path.relative_to(self.session_path)
                                archive.write(file_path, arcname=str(rel_path))
                
                # Add original uploads
                uploads_path = self.session_path / 'uploads'
                if uploads_path.exists():
                    for file_path in uploads_path.rglob('*'):
                        if file_path.is_file():
                            rel_path = file_path.relative_to(self.session_path)
                            archive.write(file_path, arcname=str(rel_path))

            self.logger.info(f"[Pipeline] Created batch output ZIP: results/batch-output.zip")
            return True
        except Exception as e:
            self.logger.error(f"[Pipeline] Failed to create batch output ZIP: {e}")
            self.results['errors'].append(f"Batch ZIP creation failed: {str(e)}")
            return False

    def run_batch(self, html_filenames, primary_lang, secondary_lang, target_lang, enable_refinement=True, refinement_mode="full"):
        """
        Run the entire pipeline (steps 1→5) for multiple files with resilient error handling.
        Always creates output ZIPs even if some steps fail.
        
        Args:
            html_filenames: List of HTML files to process
            primary_lang: Primary language of source content
            secondary_lang: Secondary language (optional)
            target_lang: Target language for translation
            enable_refinement: Whether to run GPT refinement step (default: True)
            refinement_mode: "full" (refine + harmonize), "refinement" (just refine), or "none"
        """
        session_id = self.session_id
        tracker = self.tracker
        
        self.logger.info(f"[Pipeline] Starting batch processing for {len(html_filenames)} files")
        self.logger.info(f"[Pipeline] Languages: {primary_lang} -> {target_lang} (secondary: {secondary_lang})")
        self.logger.info(f"[Pipeline] GPT Refinement: {'Enabled' if enable_refinement else 'Disabled'} (mode: {refinement_mode})")
        
        # Diagnostic: Check if core_scripts directory exists
        core_scripts_path = Path(__file__).parent.parent.parent / "core_scripts"
        self.logger.info(f"[Pipeline] Core scripts directory: {core_scripts_path}")
        self.logger.info(f"[Pipeline] Core scripts exists: {core_scripts_path.exists()}")
        
        if core_scripts_path.exists():
            scripts = list(core_scripts_path.glob("*.py"))
            self.logger.info(f"[Pipeline] Available scripts: {[s.name for s in scripts]}")
        
        # Create all required directories
        for dirname in ['extracted', 'translated', 'refined', 'final', 'results']:
            (self.session_path / dirname).mkdir(exist_ok=True)

        # ═══ STEP 1: CONTENT EXTRACTION ═══
        tracker.update(session_id, 1, "Extracting content from uploaded files...")
        
        
        for html_name in html_filenames:
            self.results['files_processed'] += 1
            basename = Path(html_name).stem
            file_extension = Path(html_name).suffix.lower()
            if file_extension == '.sql':
               file_type = "sql"
            elif file_extension == '.html':
                 file_type = "html"
            elif file_extension in ['.py', '.pyw', '.jinja', '.jinja2', '.j2']:
                 file_type = "python"
            elif file_extension == '.pdf':
                 file_type = "pdf"
            else:
                 file_type = "html"
                 self.logger.warning(f"[Pipeline] Unknown file extension {file_extension}, defaulting to HTML")
                
            
    
            
            # Prepare step 1 arguments
            input_file = self.session_path / "uploads" / html_name
            output_dir = self.session_path / "extracted" / basename
            output_dir.mkdir(exist_ok=True, parents=True)
            
            # Diagnostic: Check if input file exists
            if not input_file.exists():
                error_msg = f"Input file not found: {input_file}"
                self.logger.error(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)
                continue
            
            self.logger.info(f"[Pipeline] Input file: {input_file} (exists: {input_file.exists()}, size: {input_file.stat().st_size if input_file.exists() else 0} bytes)")
            self.logger.info(f"[Pipeline] Detected file type: {file_type}")
            

            args = [
                str(input_file),
                "--lang", primary_lang,
                "--output-dir", str(output_dir),
                "--file-type", file_type
            ]
            if secondary_lang:
                args += ["--secondary-lang", secondary_lang]
            if translate_placeholders:
                args += ["--translate-placeholders"]

            self.logger.info(f"[Pipeline] Step 1: Extracting {file_type.upper()} content from {html_name}")
  
            self.logger.info(f"[Pipeline] Step 1 args: {args}")
            
            success = self._execute_script("step1_extract.py", args)
            
            if success:
                self.results['files_extracted'] += 1
                self.logger.info(f"[Pipeline] Step 1 completed for {html_name} ({file_type.upper()} file)")
        
                
                # Check if expected output files were created
                expected_files = ['translatable_flat.json', 'translatable_structured.json', 'translatable_flat_sentences.json']
                if file_type == "html":
                    expected_files.append('non_translatable.html')
                elif file_type == "sql":
                    expected_files.append('non_translatable.sql')
                elif file_type == "python":
                    expected_files.append('non_translatable.py')
            
                for expected in expected_files:
                    file_path = output_dir / expected
                    if file_path.exists():
                        self.logger.info(f"[Pipeline] Created: {expected} ({file_path.stat().st_size} bytes)")
                    else:
                        self.logger.warning(f"[Pipeline] Missing expected output: {expected}")
            else:
                error_msg = f"Content extraction failed for {html_name} ({file_type.upper()} file)"
       
                self.logger.error(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)

        # ═══ STEP 2: TRANSLATION ═══
        tracker.update(session_id, 2, "Translating extracted content...")
        shared_memory_file = self.session_path / "translation_memory.json"
        
        for html_name in html_filenames:
            basename = Path(html_name).stem
            
            # Check if step 1 output exists
            input_json = self.session_path / "extracted" / basename / "translatable_flat.json"
            if not input_json.exists():
                error_msg = f"Skipping translation for {html_name} - no extraction output"
                self.logger.warning(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)
                continue

            # Prepare step 2 arguments  
            output_dir = self.session_path / "translated" / basename
            output_dir.mkdir(exist_ok=True, parents=True)

            args = [
                "--input", str(input_json),
                "--output", str(output_dir / "translations.json"),
                "--segments", str(output_dir / "segments_only.json"),
                "--lang", target_lang,
                "--primary-lang", primary_lang,
                "--memory", str(shared_memory_file),
                "--update-memory"              ]
            if secondary_lang:
                args += ["--secondary-lang", secondary_lang]

            self.logger.info(f"[Pipeline] Step 2: Translating {html_name}")
            self.logger.info(f"[Pipeline] Memory file: {shared_memory_file}")
            self.logger.info(f"[Pipeline] Memory file exists before: {shared_memory_file.exists()}")
            
            success = self._execute_script("step2_translate.py", args)
            
            if success:
                if shared_memory_file.exists():
                    try:
                        import json
                        with open(shared_memory_file, 'r') as f:
                            memory_data = json.load(f)
                        self.logger.info(f"[Pipeline] Memory after {basename}: {len(memory_data)} entries")
                    except Exception as e:
                        self.logger.warning(f"[Pipeline] Could not read memory file: {e}")
               
                self.results['files_translated'] += 1
                self.logger.info(f"[Pipeline] Step 2 completed for {html_name}")
            else:
                error_msg = f"Translation failed for {html_name} (API keys may be missing/invalid)"
                self.logger.error(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)

        # ═══ STEP 3: GPT REFINEMENT (CONDITIONAL) ═══
        if enable_refinement:
            if refinement_mode == "full":
                tracker.update(session_id, 3, "Refining translations with GPT (full harmonization)...")
            elif refinement_mode == "refinement":
                tracker.update(session_id, 3, "Refining translations (without harmonization)...")
                
            
            
            for html_name in html_filenames:
                basename = Path(html_name).stem
                
                # Check if step 2 outputs exist
                context_json = self.session_path / "extracted" / basename / "translatable_flat_sentences.json"
                segments_json = self.session_path / "translated" / basename / "segments_only.json"
                
                if not (context_json.exists() and segments_json.exists()):
                    error_msg = f"Skipping GPT refinement for {html_name} - missing translation outputs"
                    self.logger.warning(f"[Pipeline] {error_msg}")
                    self.results['errors'].append(error_msg)
                    continue

                # Prepare step 3 arguments
                output_dir = self.session_path / "refined" / basename
                output_dir.mkdir(exist_ok=True, parents=True)

                args = [
                    "--context", str(context_json),
                    "--translated", str(segments_json),
                    
                    "--primary-lang", primary_lang,
                    "--target-lang", target_lang,
                    "--output", str(output_dir / "openai_translations.json")
                ]
                if secondary_lang:
                    args += ["--secondary-lang", secondary_lang]
                if refinement_mode == "refinement":
                    args.append("--skip-harmonization")

                
                self.logger.info(f"[Pipeline] Step 3: GPT refining {html_name} (mode:{refinement_mode})")
                
                success = self._execute_script("step3_gpt_process.py", args)
                
                if success:
                    self.results['files_refined'] += 1
                    self.logger.info(f"[Pipeline] Step 3 completed for {html_name}")
                else:
                    error_msg = f"GPT refinement failed for {html_name} (OpenAI API key may be missing/invalid)"
                    self.logger.error(f"[Pipeline] {error_msg}")
                    self.results['errors'].append(error_msg)
        else:
            # Skip refinement step
            tracker.update(session_id, 3, "Skipping GPT refinement (disabled by user)...")
            self.logger.info("[Pipeline] Step 3: GPT refinement disabled by user configuration")
            # Set files_refined to files_translated since we're skipping this step
            self.results['files_refined'] = self.results['files_translated']
            time.sleep(1)  # Brief pause for user to see the skip message

# ═══ STEP 4: MERGE ═══
        tracker.update(session_id, 4, "Merging into final HTML/SQL files...")
        
        for file_name in html_filenames:
            basename = Path(file_name).stem
            
            # Prepare paths for merging - detect file type
            original_file = self.session_path / "uploads" / file_name
            file_extension = Path(file_name).suffix.lower()
            
            # Check for both HTML and SQL extracted templates
            non_translatable_html = self.session_path / "extracted" / basename / "non_translatable.html"
            non_translatable_sql = self.session_path / "extracted" / basename / "non_translatable.sql"
            non_translatable_py = self.session_path / "extracted" / basename / "non_translatable.py"
            non_translatable_pdf = self.session_path / "extracted" / basename / "non_translatable.pdf"
            
            
            
            # Determine which template to use
            if non_translatable_sql.exists():
                non_translatable_file = non_translatable_sql
                output_extension = ".sql"
                file_type = "SQL"
            elif non_translatable_py.exists():
                non_translatable_file = non_translatable_py
                output_extension = ".py"
                file_type = "Python"
            elif non_translatable_pdf.exists():
                non_translatable_file = non_translatable_pdf
                output_extension = ".pdf"
                file_type = "PDF"
  
            elif non_translatable_html.exists():
                non_translatable_file = non_translatable_html
                output_extension = ".html"
                file_type = "HTML"
            else:
                error_msg = f"Skipping merge for {file_name} - no extracted content template found"
                self.logger.warning(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)
                continue
            
            deepl_json = self.session_path / "translated" / basename / "segments_only.json"
            openai_json = self.session_path / "refined" / basename / "openai_translations.json"
            
            # Create final output directory
            final_dir = self.session_path / "final" / basename
            final_dir.mkdir(exist_ok=True, parents=True)
            
            # Check what files are available for merging
            has_extracted = non_translatable_file.exists()
            has_deepl = deepl_json.exists()
            has_openai = openai_json.exists() and enable_refinement  # Only consider OpenAI if refinement was enabled
            
            # Determine merge strategy based on available files
            merge_success = False
            
            if has_deepl and has_openai:
                # Full merge with both translation services
                args = [
                    "--input", str(non_translatable_file),
                    "--deepl", str(deepl_json),
                    "--openai", str(openai_json),
                    "--output-deepl", str(final_dir / f"final_deepl_{target_lang.lower()}{output_extension}"),
                    "--output-openai", str(final_dir / f"final_openai_{target_lang.lower()}{output_extension}"),
                    "--target-lang", target_lang.lower(),
                    "--both"
                ]
                
                self.logger.info(f"[Pipeline] Step 4: Full merge (DeepL + OpenAI) for {file_name} ({file_type})")
                merge_success = self._execute_script("step4_merge.py", args)
                
            elif has_deepl:
                # DeepL-only merge (either refinement disabled or failed)
                args = [
                    "--input", str(non_translatable_file),
                    "--deepl", str(deepl_json),
                    "--output-deepl", str(final_dir / f"final_deepl_{target_lang.lower()}{output_extension}"),
                    "--target-lang", target_lang.lower()
                ]
                
                refinement_status = "refinement disabled" if not enable_refinement else "OpenAI refinement failed"
                self.logger.info(f"[Pipeline] Step 4: DeepL-only merge ({refinement_status}) for {file_name} ({file_type})")
                merge_success = self._execute_script("step4_merge.py", args)
                
            else:
                # Fallback: copy original file with a note
                try:
                    fallback_path = final_dir / f"original_{basename}{file_extension}"
                    import shutil
                    shutil.copy2(original_file, fallback_path)
                    
                    # Add a note about processing status
                    note_path = final_dir / f"processing_note_{basename}.txt"
                    with open(note_path, 'w') as f:
                        f.write(f"Processing Status for {file_name}:\n")
                        f.write(f"- Content extraction: {'✓' if has_extracted else '✗'}\n")
                        f.write(f"- Translation: {'✗ (API issues)' if not has_deepl else '✓'}\n")
                        refinement_note = "✗ (disabled)" if not enable_refinement else "✗ (API issues)"
                        f.write(f"- GPT refinement: {refinement_note}\n")
                        f.write(f"\nOriginal file preserved as: original_{basename}{file_extension}\n")
                    
                    merge_success = True
                    self.logger.info(f"[Pipeline] Step 4: Fallback preservation for {file_name}")
                    
                except Exception as e:
                    self.logger.error(f"[Pipeline] Failed to preserve original file {file_name}: {e}")
                    merge_success = False
            
            if merge_success:
                self.results['files_merged'] += 1
                self.logger.info(f"[Pipeline] Step 4 completed for {file_name}")
            else:
                error_msg = f"Final merge failed for {file_name}"
                self.logger.error(f"[Pipeline] {error_msg}")
                self.results['errors'].append(error_msg)


        # ═══ GENERATE MEMORY EFFICIENCY REPORT ═══
        self.logger.info("[Pipeline] Generating memory efficiency report...")
        self._generate_memory_report()
        
        # ═══ GENERATE CONSOLIDATED METRICS REPORT ═══
        self.logger.info("[Pipeline] Generating consolidated metrics report...")
        self._generate_consolidated_metrics_report(html_filenames, enable_refinement)
        
        # ═══ STEP 5: PACKAGING ═══
        tracker.update(session_id, 5, "Packaging ZIP archives...")
        
        # Create individual file ZIPs
        for html_name in html_filenames:
            basename = Path(html_name).stem
            # Only create per-file ZIP if we have a final directory for this file
            final_dir = self.session_path / "final" / basename
            if final_dir.exists() and any(final_dir.iterdir()):
                self._create_per_file_zip(basename, self.session_path / "results", self.session_path / "final")

        # Create comprehensive batch output ZIP
        self.logger.info("[Pipeline] Creating comprehensive batch output ZIP...")
        batch_zip_success = self._create_batch_output_zip(self.session_path / "results")

        
        

        # ═══ FINAL STATUS UPDATE (CRITICAL: Only after ZIPs are created) ═══
        self._finalize_processing(session_id, tracker, enable_refinement)
        
        return True  # Always return True since we create outputs even with partial failures

    def _finalize_processing(self, session_id, tracker, enable_refinement=True):
        """Determine final status and update tracker - ONLY after ZIPs are written"""
        total_files = self.results['files_processed']
        successful_files = self.results['files_merged']
        error_count = len(self.results['errors'])
        
        self.logger.info(f"[Pipeline] Processing summary:")
        self.logger.info(f"  Total files: {total_files}")
        self.logger.info(f"  Successfully extracted: {self.results['files_extracted']}")
        self.logger.info(f"  Successfully translated: {self.results['files_translated']}")
        self.logger.info(f"  Successfully refined: {self.results['files_refined']} (refinement {'enabled' if enable_refinement else 'disabled'})")
        self.logger.info(f"  Successfully merged: {self.results['files_merged']}")
        self.logger.info(f"  Errors encountered: {error_count}")

        # CRITICAL FIX: Ensure filesystem sync before marking complete
        if hasattr(os, 'sync'):
            os.sync()  # Force filesystem to flush all pending writes
        time.sleep(0.2)  # Give filesystem a moment to complete operations

        if error_count == 0:
            # Perfect success - ZIPs are now guaranteed to be on disk
            tracker.complete(session_id)
        elif successful_files > 0:
            # Partial success - ZIPs are now guaranteed to be on disk
            status_msg = f"{successful_files}/{total_files} files processed successfully"
            if error_count > 0:
                status_msg += f" ({error_count} errors)"
            tracker.complete_partial(session_id, status_msg)
        else:
            # Complete failure
            error_summary = "; ".join(self.results['errors'][:3])  # Show first 3 errors
            if len(self.results['errors']) > 3:
                error_summary += f" and {len(self.results['errors']) - 3} more..."
            tracker.fail(session_id, "processing_failed", error_summary)

        self.logger.info(f"[Pipeline] Final check - Session path still exists: {self.session_path.exists()}")
        if self.session_path.exists():
            self.logger.info(f"[Pipeline] Session folder contents:{list(self.session_path.iterdir())}")
    

    
    def get_processing_summary(self):
        """Get summary of processing results"""
        return {
            'total_files': self.results['files_processed'],
            'extracted': self.results['files_extracted'],
            'translated': self.results['files_translated'],
            'refined': self.results['files_refined'],
            'merged': self.results['files_merged'],
            'errors': self.results['errors']
        }