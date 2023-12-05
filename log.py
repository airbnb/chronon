import os
import re

def process_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Check if the file contains println
    contains_println = any('println' in line for line in lines)
    if not contains_println:
        return  # No need to modify the file

    # Prepare the import statement and logger instance
    import_statement = 'import org.slf4j.LoggerFactory\n'
    logger_instance = '  private val logger = LoggerFactory.getLogger(getClass)\n'

    # Determine where to insert the import
    import_index = next((i for i, line in enumerate(lines) if line.startswith('import')), 0)
    if import_index != 0 or not any(line.startswith('import org.slf4j.LoggerFactory') for line in lines):
        lines.insert(import_index, import_statement)
        import_index += 1

    # Regex to match class or object definitions
    object_or_class_regex = re.compile(r'\b(object|class)\s+\w+')

    # Insert logger instance after the opening brace of each class or object
    for i in range(len(lines)):
        if object_or_class_regex.search(lines[i]):
            # Find the opening brace
            brace_index = i
            while brace_index < len(lines) and '{' not in lines[brace_index]:
                brace_index += 1
            if brace_index < len(lines):
                lines.insert(brace_index + 1, logger_instance)

    # Replace println with logger.info
    updated_content = ''.join(lines).replace('println', 'logger.info')

    # Write the updated content
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
