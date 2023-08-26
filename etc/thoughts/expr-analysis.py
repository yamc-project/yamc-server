# this code can be used to analyse an expression in data fields to understand which provider's classes
# are used in the expression

# the ast tree will have to be combine with the scope in which the expression is evaluated
# to determine which provider's classes are used in the expression

import ast


# Define a visitor class to traverse the AST and extract class names
class ExprVisitor(ast.NodeVisitor):
    def __init__(self):
        self.class_names = []

    def visit_Name(self, node):
        if node.id.isidentifier():
            self.class_names.append(node.id)

    def visit_Attribute(self, node):
        self.visit(node.value)  # Traverse the left side of the attribute
        self.class_names.append(node.attr)

    def visit_Call(self, node):
        self.visit(node.func)  # Visit the function being called
        for arg in node.args:
            self.visit(arg)  # Visit arguments to the function call


if isinstance(collector.data_def, PythonExpression):
    code = collector.data_def.code()
    visitor = ExprVisitor()
    visitor.visit(code.body[0])
    print(visitor.class_names)
    exit()
