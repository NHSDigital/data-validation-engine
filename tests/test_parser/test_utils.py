import pytest
from dve.metadata_parser.utilities import resilient_get

class MyParent:
    cls_attr = "hello"
    def __init__(self, my_attr:str, another_attr:str):
        self.my_attr = my_attr
        self.another_attr = another_attr

class MyObject(MyParent):
    sub_attr = "bye"
    def __init__(self, extra_attr:int):
        self.extra_attr = extra_attr
        super().__init__("from", "child")
    

@pytest.mark.parametrize("obj,attrs,expected", [(MyParent, ("cls_attr",), "hello"),
                                                (MyObject, ("cls_attr", "sub_attr"), "hello"),
                                                (MyObject, ("sub_attr", "cls_attr"), "bye"),
                                                (MyParent, ("my_attr",), None),
                                                (MyParent("this", "test"), ("extra_attr", "my_attr"), "this"),
                                                (MyObject("this"), ("daft_attr", "another_daft_attr", "yet_another", "another_attr"), "child")])
def test_resilient_get(obj, attrs, expected):
    assert resilient_get(obj, *attrs) == expected