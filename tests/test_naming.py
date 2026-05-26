"""Tests for common_libs.naming module"""

from common_libs.naming import camel_to_snake, clean_obj_name, to_class_name


class TestCleanObjName:
    """Tests for clean_obj_name function"""

    def test_valid_name_passes_through(self) -> None:
        """Test that a valid name is returned unchanged"""
        name = "valid_name"
        assert clean_obj_name(name) == name

    def test_spaces_replaced_with_underscores(self) -> None:
        """Test that spaces are replaced with underscores"""
        assert clean_obj_name("my name") == "my_name"

    def test_special_chars_replaced(self) -> None:
        """Test that special characters are replaced with underscores"""
        assert clean_obj_name("my@name#here") == "my_name_here"

    def test_starts_with_digit_gets_underscore_prefix(self) -> None:
        """Test that a name starting with a digit gets an underscore prefix"""
        result = clean_obj_name("123name")
        assert result.startswith("_")
        assert "123" in result

    def test_reserved_keywords_get_underscore_prefix(self) -> None:
        """Test that reserved keywords get an underscore prefix"""
        assert clean_obj_name("class") == "_class"
        assert clean_obj_name("def") == "_def"
        assert clean_obj_name("import") == "_import"

    def test_consecutive_special_chars_become_single_underscore(self) -> None:
        """Test that multiple consecutive special chars collapse to a single underscore"""
        assert clean_obj_name("a@@b##c") == "a_b_c"


class TestCamelToSnake:
    """Tests for camel_to_snake function"""

    def test_camel_case(self) -> None:
        """Test that camelCase is converted to snake_case"""
        assert camel_to_snake("camelCase") == "camel_case"

    def test_pascal_case(self) -> None:
        """Test that PascalCase is converted to snake_case"""
        assert camel_to_snake("PascalCase") == "pascal_case"

    def test_already_snake_case(self) -> None:
        """Test that snake_case input is returned as-is (lowercased)"""
        assert camel_to_snake("snake_case") == "snake_case"

    def test_all_lowercase(self) -> None:
        """Test that a lowercase string is returned as-is"""
        assert camel_to_snake("alreadylower") == "alreadylower"

    def test_digit_before_uppercase(self) -> None:
        """Test that a digit before an uppercase letter gets a separator"""
        assert camel_to_snake("value1Other") == "value1_other"

    def test_consecutive_uppercase_acronym(self) -> None:
        """Test that consecutive uppercase letters (acronyms) are not split.

        The regex only inserts '_' at lowercase/digit → uppercase boundaries,
        so 'HTTPRequest' has no such boundary and collapses to 'httprequest'.
        This is a known limitation of the current implementation.
        """
        assert camel_to_snake("HTTPRequest") == "httprequest"

    def test_empty_string(self) -> None:
        """Test that an empty string returns an empty string"""
        assert camel_to_snake("") == ""


class TestToClassName:
    """Tests for to_class_name function"""

    def test_space_separated_words(self) -> None:
        """Test that space-separated words are converted to PascalCase"""
        assert to_class_name("my class") == "MyClass"

    def test_single_leading_underscore_preserved(self) -> None:
        """Test that a single leading underscore is preserved"""
        assert to_class_name("_private_name") == "_PrivateName"

    def test_multiple_leading_underscores_preserved(self) -> None:
        """Test that multiple leading underscores are preserved"""
        assert to_class_name("__double") == "__Double"

    def test_snake_case_input(self) -> None:
        """Test that snake_case input is converted to PascalCase"""
        assert to_class_name("my_class_name") == "MyClassName"

    def test_camel_case_input(self) -> None:
        """Test that camelCase input round-trips through snake and produces PascalCase"""
        assert to_class_name("alreadyCamel") == "AlreadyCamel"

    def test_prefix(self) -> None:
        """Test that prefix is prepended to the class name"""
        assert to_class_name("test", prefix="My") == "MyTest"

    def test_suffix(self) -> None:
        """Test that suffix is appended to the class name"""
        assert to_class_name("test", suffix="Model") == "TestModel"

    def test_prefix_and_suffix(self) -> None:
        """Test that prefix and suffix are both applied"""
        assert to_class_name("test", prefix="My", suffix="Model") == "MyTestModel"

    def test_starts_with_digit_gets_underscore_prefix(self) -> None:
        """Test that a base name starting with a digit produces a leading underscore"""
        assert to_class_name("123foo") == "_123foo"

    def test_reserved_keyword_capitalized_is_valid(self) -> None:
        """Test that a reserved keyword input becomes a valid capitalized class name"""
        assert to_class_name("class") == "Class"

    def test_empty_string(self) -> None:
        """Test that an empty string returns an empty string"""
        assert to_class_name("") == ""
