load("@rules_python//python:defs.bzl", "py_library")

def _thrift_java_library_impl(ctx):
    thrift_path = ctx.attr.thrift_binary
    outputs = []
    commands = []

    for src in ctx.files.srcs:
        # Generate unique output filenames
        basename = src.basename.replace(".thrift", "")
        output = ctx.actions.declare_file("{}_{}-java.srcjar".format(ctx.attr.name, basename))
        outputs.append(output)

        # Temporary directory for generated Java code
        gen_java_dir = "{}-tmp".format(output.path)

        # Command for generating Java from Thrift
        command = """
        mkdir -p {gen_java_dir} && \
        {thrift_path} -strict -gen java -out {gen_java_dir} {src} && \
        find {gen_java_dir} -exec touch -t 198001010000 {{}} + && \
        jar cf {gen_java} -C {gen_java_dir} . && \
        rm -rf {gen_java_dir}
        """.format(
            gen_java_dir = gen_java_dir,
            thrift_path = thrift_path,
            src = src.path,
            gen_java = output.path,
        )

        # Adjust command for Windows if necessary
        if ctx.configuration.host_path_separator == ";":
            command = command.replace("&&", "&").replace("/", "\\")

        commands.append(command)

    # Combine all commands into a single shell command
    combined_command = " && ".join(commands)

    ctx.actions.run_shell(
        outputs = outputs,
        inputs = ctx.files.srcs,
        command = combined_command,
        progress_message = "Generating Java code from {} Thrift files".format(len(ctx.files.srcs)),
    )

    return [DefaultInfo(files = depset(outputs))]

_thrift_java_library = rule(
    implementation = _thrift_java_library_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".thrift"],
            mandatory = True,
            doc = "List of .thrift source files",
        ),
        "thrift_binary": attr.string(),
    },
)

def thrift_java_library(name, srcs, **kwargs):
    _thrift_java_library(
        name = name,
        srcs = srcs,
        thrift_binary = select({
            "@platforms//os:macos": "/usr/local/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
        **kwargs
    )

def _thrift_python_library_impl(ctx):
    thrift_binary = ctx.attr.thrift_binary
    all_outputs = []
    commands = []

    for src in ctx.files.srcs:
        # Get base name without .thrift extension
        base_name = src.basename.replace(".thrift", "")

        # Convert namespace to directory structure
        namespace_dir = ctx.attr.namespace.replace(".", "/")

        # Declare output directory matching the namespace structure
        output_dir = "{}/{}".format(namespace_dir, base_name)
        main_py = ctx.actions.declare_file("{}/{}.py".format(output_dir, base_name))
        constants_py = ctx.actions.declare_file("{}/constants.py".format(output_dir))
        ttypes_py = ctx.actions.declare_file("{}/ttypes.py".format(output_dir))
        module_init = ctx.actions.declare_file("{}/__init__.py".format(output_dir))

        file_outputs = [main_py, constants_py, ttypes_py, module_init]
        all_outputs.extend(file_outputs)

        # Command to generate files in the correct namespace
        command = """
            mkdir -p {output_dir} && \
            {thrift_binary} --gen py:package_prefix={namespace}. -out $(dirname {output_dir}) {src} && \
            touch {main_py} {constants_py} {ttypes_py} {module_init}
        """.format(
            thrift_binary = thrift_binary,
            namespace = ctx.attr.namespace,
            output_dir = main_py.dirname,
            src = src.path,
            main_py = main_py.path,
            constants_py = constants_py.path,
            ttypes_py = ttypes_py.path,
            module_init = module_init.path,
        )
        commands.append(command)

    combined_command = " && ".join(commands)

    print("commands: {}".format(combined_command))

    # Generate files
    ctx.actions.run_shell(
        outputs = all_outputs,
        inputs = ctx.files.srcs,
        command = combined_command,
        progress_message = "Generating Python code from Thrift files: %s" % ", ".join([src.path for src in ctx.files.srcs]),
    )

    return [DefaultInfo(files = depset(all_outputs))]

_thrift_python_library_gen = rule(
    implementation = _thrift_python_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".thrift"]),
        "thrift_binary": attr.string(),
        "namespace": attr.string(),
    },
)

def thrift_python_library(name, srcs, namespace, visibility = None):
    """Generates Python code from Thrift files with correct namespace structure."""
    _thrift_python_library_gen(
        name = name + "_gen",
        srcs = srcs,
        namespace = namespace,
        thrift_binary = select({
            "@platforms//os:macos": "/usr/local/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
    )

    py_library(
        name = name,
        srcs = [":" + name + "_gen"],
        imports = ["."],
        visibility = visibility,
    )
