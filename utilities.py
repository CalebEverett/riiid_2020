import os
import subprocess

def porc(c):
    print_output(run_command(c))

def print_output(output):
    """Prints output from string."""
    for l in output.split('\n'):
        print(l)

def run_command(command):
    """Runs command line command as a subprocess returning output as string."""
    STDOUT = subprocess.PIPE
    process = subprocess.run(command, shell=True, check=False,
                             stdout=STDOUT, stderr=STDOUT, universal_newlines=True)
    
    output = process.stdout if process.stdout else process.stderr
    
    return output

def get_v_tuple(v):
    return tuple([int(s) for s in v.split('.')])

def check_package(p, v):
    output = run_command(f'pip freeze | grep {p}')
    if output == '':
        porc(f'pip install {p}')
    elif get_v_tuple(output[output.find('==')+2:]) < get_v_tuple(v):
        porc(f'pip install -U {p}')
    else:
        print_output(output)

def check_packages(packages):
    for p, v in packages.items():
        check_package(p, v)

class Git:
    def __init__(self, repo, username, password, email):
        self.repo = repo
        self.username = username
        self.password = password
        self.email = email

    def clone(self):
        cred_repo = (
            f'https://{self.username}:{self.password}'
            f'@github.com/{self.username}/{self.repo}.git'
        )

        commands = []
        commands.append(f'git config --global user.email {self.email}')
        commands.append(f'git config --global user.name {self.username}')
        commands.append(f'git clone {cred_repo}')
        for cmd in commands:
            porc(cmd)

    def commit(self, message='made some changes'):
        cwd = os.getcwd()
        os.chdir(self.repo)
        porc('git add -A')
        porc(f'git commit -m "{message}"')
        os.chdir(cwd)

    def command(self, command):
        cwd = os.getcwd()
        os.chdir(self.repo)
        porc(f'git {command}')
        os.chdir(cwd)

    def status(self):
        self.command('status')

    def push(self):
        self.command('push origin master')