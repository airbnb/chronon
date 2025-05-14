##### Begin bazel-flag-hack
# The flag hack stuff below is a way to detect flags that bazel has been invoked with from the
# aspect. Once PY3-as-default is stable, it can be removed. When removing, also remove the
# define_flag_hack() call in BUILD and the "_flag_hack" attr on the aspect below. See
# "PY3-as-default" in:
# https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/rules/python/PythonConfiguration.java

FlagHackInfo = provider(fields = ["incompatible_py2_outputs_are_suffixed"])

def _flag_hack_impl(ctx):
    return [FlagHackInfo(incompatible_py2_outputs_are_suffixed = ctx.attr.incompatible_py2_outputs_are_suffixed)]

_flag_hack_rule = rule(
    attrs = {"incompatible_py2_outputs_are_suffixed": attr.bool()},
    implementation = _flag_hack_impl,
)

def define_flag_hack():
    native.config_setting(
        name = "incompatible_py2_outputs_are_suffixed_setting",
        values = {"incompatible_py2_outputs_are_suffixed": "true"},
    )
    _flag_hack_rule(
        name = "flag_hack",
        incompatible_py2_outputs_are_suffixed = select({
            ":incompatible_py2_outputs_are_suffixed_setting": True,
            "//conditions:default": False,
        }),
        visibility = ["//visibility:public"],
    )

##### End bazel-flag-hack
