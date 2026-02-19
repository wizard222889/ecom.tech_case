from utils.generate_data import DataGenerator

def test_generate_count():
    gen = DataGenerator().generate_users(20)
    assert len(gen) == 20

def test_generate_col():
    gen = DataGenerator().generate_users(20)
    assert "name" in gen.columns    