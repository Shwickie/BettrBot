#!/usr/bin/env python3
"""
Startup script to detect and fix model compatibility issues
This will automatically regenerate the model if pandas version mismatch is detected
"""
import os
import sys
import pickle
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_model_compatibility():
    """Check if the model pickle is compatible with current pandas version"""
    model_paths = [
        "/opt/render/project/src/betting_model_fixed.pkl",
        "betting_model_fixed.pkl",
        "models/betting_model_fixed.pkl"
    ]

    for model_path in model_paths:
        if os.path.exists(model_path):
            logger.info(f"Found model at {model_path}")
            try:
                with open(model_path, 'rb') as f:
                    model_data = pickle.load(f)
                logger.info("✅ Model loaded successfully - pandas version is compatible")
                return True
            except ModuleNotFoundError as e:
                if 'pandas.core.indexes.numeric' in str(e):
                    logger.error(f"❌ Model incompatible with current pandas version: {e}")
                    logger.info(f"Deleting incompatible model: {model_path}")
                    os.remove(model_path)
                    return False
                else:
                    logger.error(f"Unexpected error loading model: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error checking model: {e}")
                raise

    logger.warning("No model found - will need to regenerate")
    return False

def regenerate_model():
    """Regenerate the model with current pandas version"""
    logger.info("=" * 60)
    logger.info("REGENERATING MODEL WITH CURRENT PANDAS VERSION")
    logger.info("=" * 60)

    # Import and run the training script
    try:
        # Add parent directory to path
        parent_dir = os.path.join(os.path.dirname(__file__), '..')
        sys.path.insert(0, os.path.join(parent_dir, 'model'))

        from train_betting_model import main as train_main
        train_main()
        logger.info("✅ Model regenerated successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to regenerate model: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logger.info("Checking model compatibility...")

    if not check_model_compatibility():
        logger.info("Model needs regeneration")
        if regenerate_model():
            logger.info("✅ Model fix complete")
            sys.exit(0)
        else:
            logger.error("❌ Model fix failed")
            sys.exit(1)
    else:
        logger.info("✅ Model is compatible - no action needed")
        sys.exit(0)
