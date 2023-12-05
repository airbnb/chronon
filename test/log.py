import os
import re

def process_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Regex to match class or object (excluding case classes) definitions
    object_or_class_regex = re.compile(r'\b(?<!case\s)(object|class)\s+(\w+)')
    println_regex = re.compile(r'\bprintln\b')

    # Prepare the import statement and logger instance
    import_statement = 'import org.slf4j.LoggerFactory\n'
    logger_instance = '  private val logger = LoggerFactory.getLogger(getClass)\n'
    need_import = False

    # Process each class or object for println
    i = 0
    while i < len(lines):
        match = object_or_class_regex.search(lines[i])
        if match:
            class_or_object_name = match.group(2)
            # Determine the block of the current class or object
            start_index = i
            brace_count = 0
            while i < len(lines) and (brace_count > 0 or not lines[i].strip().endswith('}')):
                if '{' in lines[i]:
                    brace_count += lines[i].count('{')
                if '}' in lines[i]:
                    brace_count -= lines[i].count('}')
                i += 1
            end_index = i

            # Check if this block contains println
            block_lines = lines[start_index:end_index]
            if any(println_regex.search(line) for line in block_lines):
                need_import = True
                # Find the opening brace and insert logger after all header lines
                brace_index = next((j for j, line in enumerate(block_lines) if '{' in line), len(block_lines) - 1)
                block_lines.insert(brace_index + 1, logger_instance)
                # Replace println within this block
                block_lines = [line.replace('println', 'logger.info') for line in block_lines]
                lines[start_index:end_index] = block_lines
        else:
            i += 1

    # Add import statement if needed
    if need_import:
        import_index = next((i for i, line in enumerate(lines) if line.startswith('import')), 0)
        if not any(line.startswith('import org.slf4j.LoggerFactory') for line in lines):
            lines.insert(import_index, import_statement)

    # Write the updated content
    updated_content = ''.join(lines)
    with open(file_path, 'w') as file:
        file.write(updated_content)

def search_and_replace(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.scala'):
                process_file(os.path.join(root, file))

# Run from the current directory
current_directory = os.getcwd()
search_and_replace(current_directory)

print("Replacement complete.")
