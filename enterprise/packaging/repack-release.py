import argparse
import tarfile
import os
import sys
import shutil

def rename_files(dir, args):
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.startswith("taos") and (not file.endswith(".h")): 
                file_path = os.path.join(root, file)
                new_file = file.replace("taos", args.prompt)
                new_file_path = os.path.join(root, new_file)
                os.rename(file_path, new_file_path)

def replace_contents(dir, args):
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".cfg"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                if "support@taosdata.com" in content:
                    content = content.replace("support@taosdata.com", args.email)
                    print("replaced _support@taosdata.com_ with _"+args.email+"_ in "+file_path)
                if "taos" in content:
                    content = content.replace("taos", args.prompt)
                    print("replaced _taos_ with _"+args.prompt+"_ in "+file_path)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith(".toml"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                if "log/taos" in content:
                    content = content.replace("log/taos", "log/"+args.prompt)
                    print("replaced _log/taos_ with _log/"+args.prompt+"_ in "+file_path)
                if "lib/taos" in content:
                    content = content.replace("lib/taos", "lib/"+args.prompt)
                    print("replaced _lib/taos_ with _lib/"+args.prompt+"_ in "+file_path)
                if "TDengine" in content:
                    content = content.replace("TDengine", args.name)
                    print("replaced _TDengine_ with _"+args.name+"_ in "+file_path)
                if "taosAdapter" in content:
                    content = content.replace("taosAdapter that", args.prompt+"Adapter that")
                    print("replace _taosAdapter that_ with _"+args.prompt+"Adapter that_ in "+file_path)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith(".service"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("taos", args.prompt)
                content = content.replace("Taos", args.prompt)
                content = content.replace("TDengine", args.name)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith("adapter.service"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("ExecStart=/usr/bin/"+args.prompt+"adapter", "ExecStart=/usr/bin/"+args.prompt+"adapter -c /etc/"+args.prompt+"/"+args.prompt+"adapter.toml")
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith("keeper.service"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("ExecStart=/usr/bin/"+args.prompt+"keeper", "ExecStart=/usr/bin/"+args.prompt+"keeper -c /etc/"+args.prompt+"/keeper.toml")
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith("install.sh"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("taosKeeper", args.prompt+"Keeper")
                content = content.replace("taoskeeper", args.prompt+"keeper")
                content = content.replace("taosadapter", args.prompt+"adapter")
                content = content.replace("usr/local/taos", "usr/local/"+args.prompt)
                content = content.replace("rmtaos", "rm"+args.prompt)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith("install_client.sh"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("configDir=\"/etc/taos\"", "configDir=\"/etc/"+args.prompt+"\"")
                content = content.replace("taos.cfg", args.prompt+".cfg")
                content = content.replace("taosdemo", args.prompt+"demo")
                content = content.replace("usr/local/taos", "usr/local/"+args.prompt)
                content = content.replace("rmtaos", "rm"+args.prompt)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.endswith("-tools.sh"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("taostools", args.prompt+"tools")
                content = content.replace("taosdemo", args.prompt+"demo")
                content = content.replace("taosBenchmark", args.prompt+"Benchmark")
                content = content.replace("taosdump", args.prompt+"dump")
                content = content.replace("usr/local/taos", "usr/local/"+args.prompt)
                content = content.replace("rmtaos", "rm"+args.prompt)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()

            if file.startswith("remove"):
                file_path = os.path.join(root, file)
                fin = open(file_path, "r")
                content = fin.read()
                content = content.replace("clientName2=\"taos\"", "clientName2=\""+args.prompt+"\"")
                content = content.replace("configFile2=\"taos.cfg\"", "configFile2=\""+args.prompt+".cfg\"")
                content = content.replace("taosKeeper", args.prompt+"Keeper")
                content = content.replace("taoskeeper", args.prompt+"keeper")
                content = content.replace("taosBenchmark", args.prompt+"Benchmark")
                content = content.replace("taosdump", args.prompt+"dump")
                content = content.replace("rmtaos", "rm"+args.prompt)
                content = content.replace("taosd", args.prompt+"d")
                content = content.replace("usr/local/taos", "usr/local/"+args.prompt)
                fin.close()

                fout = open(file_path, "w")
                fout.write(content)
                fout.close()


def repack_tar_tools(args, output_dir):
    targz_file = ""
    targz_filepath = ""
    for file in os.listdir(output_dir):
        if (file.startswith(args.prompt + "Tools")):
            targz_file = file
            targz_filepath = os.path.join(output_dir, file)
            break;

    if targz_file == "":
        print("WARN: " + args.prompt + "Tools ... .tar.gz not found in " + output_dir)
        return

    if targz_filepath == "":
        print("WARN: " + targz_filepath)
        return

    tar = tarfile.open(targz_filepath)
    tar.extractall(output_dir+"/tmp")
    tar.close()

    extract_dir = output_dir + "/tmp/" + targz_file.replace("-Linux-x64-comp3.tar.gz", "")

    replace_contents(extract_dir, args)
    rename_files(extract_dir, args)

    current_dir = os.getcwd()
    os.chdir(output_dir + "/tmp")
    with tarfile.open(targz_filepath, "w:gz") as tar:
        tar.add(os.curdir, arcname=os.curdir)
    os.chdir(current_dir)
    shutil.rmtree(output_dir+"/tmp")
    print("done to repack " + targz_file)


def repack_tar(args, pkg, output_dir):
    targz_file =  output_dir + args.name + "-enterprise-" + pkg + "-" + args.version + "-Linux-x64.tar.gz"
    if not os.path.exists(targz_file):
        print("ERROR: " + targz_file + "not found ")
        return
    tar = tarfile.open(targz_file)
    tar.extractall(output_dir)
    tar.close()

    extract_dir = output_dir + args.name + "-enterprise-" + pkg + "-" + args.version
    examples_dir =  extract_dir + "/examples"
    if os.path.exists(examples_dir):
        shutil.rmtree(examples_dir)

    connector_dir =  extract_dir + "/connector"
    if os.path.exists(connector_dir):
        shutil.rmtree(connector_dir)

    package_file = extract_dir + "/package.tar.gz"
    package_tar = tarfile.open(package_file)
    package_tar.extractall(extract_dir+"/tmp")
    package_tar.close()

    replace_contents(extract_dir, args)

    packagetar_dir = extract_dir + "/tmp"
    rename_files(packagetar_dir, args)

    current_dir = os.getcwd()
    os.chdir(packagetar_dir)
    with tarfile.open(package_file, "w:gz") as tar:
        tar.add(packagetar_dir, arcname=os.curdir)
    os.chdir(current_dir)
    shutil.rmtree(packagetar_dir)
#    sys.exit(0)

    with tarfile.open(targz_file, "w:gz") as tar:
        tar.add(extract_dir, arcname=os.path.basename(extract_dir))
    shutil.rmtree(extract_dir)

    print("done to repack " + pkg)

if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f' '--tar_file', type=str, required=False)
    parse.add_argument('-d', '--dir', type=str, required=True)
    parse.add_argument('-n', '--name', type=str, required=True)
    parse.add_argument('-p', '--prompt', type=str, required=True)
    parse.add_argument('-e', '--email', type=str, required=True)
    parse.add_argument('-v', '--version', type=str, required=True)
    args = parse.parse_args()
    print(args.name, args.prompt, args.email, args.dir, args.version)

    print("### Current dir: " + os.getcwd())
    output_dir = os.getcwd() + "/" + args.dir +"/"

    repack_tar(args, "server", output_dir)
    repack_tar(args, "client", output_dir)
    repack_tar_tools(args, output_dir)

