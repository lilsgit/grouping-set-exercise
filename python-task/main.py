import subprocess


def run_script(script_path):
    try:
        output = subprocess.check_output(['python', script_path], universal_newlines=True)
        print(f'Script {script_path} executed successfully.')
        return output
    except subprocess.CalledProcessError as e:
        print(f'Error executing script {script_path}: {e}')
        return None


def main():
    script_paths = ['./scripts/f1_pandas.py', './scripts/f2_beam.py', './scripts/f3_pyspark.py']

    for script_path in script_paths:
        output = run_script(script_path)
        if output is not None:
            print(f'Output of script {script_path}:')
            print(output)

    print('All scripts executed.')


if __name__ == '__main__':
    main()
