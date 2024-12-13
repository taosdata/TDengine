# How to Write Case Docstrings

## Python Cases

* Write a description for class:

```python
    class TDTestCase(TBase):
    """
    Here is the class description for the whole file cases
    """
```

* Write a description for function:

```python
    class TDTestCase(TBase):
    """
    Here is the class description for the whole python file cases
    """
    def test_case1():   # case function should be named start with test_
    """
    Here is the function description for single test
    """
```

* Add python file case docstrings to website:

Find the corresponding Markdown file in the docs directory for the test case file's directory, and add a line to the file.
```
    ::: path.to.your.file.file_name(without .py)
    e.g.
    ::: army.query.test_join
```

# Tsim Cases

* Write a description for sim file:

```
    ## \brief Test insert into table use view data
    sql connect
    sql use testa;
```

* Add sim file case docstrings to website:

    Find the corresponding Markdown file in the docs directory for the test case file's directory, and add a line to the file.
```
    ::: path/to/your/file/file_name.sim
        handler: shell
    e.g.
    ::: script/tsim/view/view.sim
        handler: shell
```