# thrift.bzl

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
            "@platforms//os:macos": "/usr/local/opt/thrift@0.13/bin/thrift",
            "//conditions:default": "/usr/local/bin/thrift",
        }),
        **kwargs
    )
