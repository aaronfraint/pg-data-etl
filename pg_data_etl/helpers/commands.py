import subprocess


def run_command_in_shell(command: str) -> str:
    """Use subprocess to execute a command in a shell"""

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    output = process.communicate()
    print(output[0])

    process.kill()

    return output
