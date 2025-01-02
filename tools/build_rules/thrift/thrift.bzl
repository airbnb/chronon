def _thrift_java_library_impl(ctx):
    src = ctx.file.src
    gen_java_dir = ctx.outputs.genjava.path + "-tmp"

    # Create the directory for the generated Java code
    ctx.actions.run_shell(
        outputs = [ctx.outputs.genjava],
        inputs = [src, ctx.executable.thrift_binary],
        command = """
        mkdir -p {gen_java_dir} && \
        {thrift_binary} -strict -gen java -out {gen_java_dir} {src} && \
        find {gen_java_dir} -exec touch -t 198001010000 {{}} + && \
        jar cf {gen_java} -C {gen_java_dir} .
        """.format(
            gen_java_dir = gen_java_dir,
            thrift_binary = ctx.executable.thrift_binary.path,
            src = src.path,
            gen_java = ctx.outputs.genjava.path,
        ),
        progress_message = "Generating Java code from Thrift file {}".format(src.path),
    )

    return [DefaultInfo(files = depset([ctx.outputs.genjava]))]

thrift_java_library = rule(
    implementation = _thrift_java_library_impl,
    attrs = {
        "src": attr.label(allow_single_file = [".thrift"]),
        "thrift_binary": attr.label(
            executable = True,
            cfg = "host",
            default = "//tools/build_rules/thrift:thrift_binary",
        ),
    },
    outputs = {
        "genjava": "%{name}_gen-java.srcjar",
    },
)
