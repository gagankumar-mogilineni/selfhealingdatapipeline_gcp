import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import json

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.vertex_ai_handler import analyze_error
from utils.auto_healer import apply_fix

class TestSelfHealing(unittest.TestCase):

    @patch('utils.vertex_ai_handler.vertexai')
    @patch('utils.vertex_ai_handler.TextGenerationModel')
    def test_analyze_error(self, mock_model, mock_vertexai):
        # Mock Vertex AI response
        mock_response = MagicMock()
        mock_response.text = json.dumps({
            "root_cause": "Syntax error",
            "fix_type": "CODE",
            "suggested_fix": "Fix indentation at line 10"
        })
        
        mock_llm = MagicMock()
        mock_llm.predict.return_value = mock_response
        mock_model.from_pretrained.return_value = mock_llm
        
        result = analyze_error("SyntaxError: unexpected indent", "test-project")
        
        self.assertEqual(result['fix_type'], "CODE")
        self.assertEqual(result['suggested_fix'], "Fix indentation at line 10")

    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    def test_apply_fix_code(self, mock_file):
        fix_suggestion = {
            "fix_type": "CODE",
            "suggested_fix": "import pyspark"
        }
        
        apply_fix(fix_suggestion, "dag.py", "script.py")
        
        # Verify file was opened and written to
        mock_file.assert_called_with("script.py", "a")
        mock_file().write.assert_called()

if __name__ == '__main__':
    unittest.main()
