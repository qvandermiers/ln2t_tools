#!/bin/bash
# Example usage of ln2t_tools import functionality

# Example 1: Basic DICOM import
echo "Example 1: Basic DICOM import"
ln2t_tools import \
  --dataset 2024-Colorful_Bear-6f9c8e8bfe82 \
  --participant-label 001 002 003 \
  --datatype dicom \
  --ds-initials CB

# Example 2: Import with sessions
echo "Example 2: Multi-session import"
ln2t_tools import \
  --dataset 2024-Happy_Penguin-abc123 \
  --participant-label 001 002 003 \
  --session 1 \
  --ds-initials HP \
  --datatype all

# Example 3: Import with source compression
echo "Example 3: Import with source compression"
ln2t_tools import \
  --dataset 2024-Colorful_Bear-6f9c8e8bfe82 \
  --participant-label 004 005 \
  --ds-initials CB \
  --compress-source

# Example 4: MRS data only
echo "Example 4: MRS data import"
ln2t_tools import \
  --dataset 2024-Happy_Penguin-abc123 \
  --participant-label 003 005 \
  --session 1 \
  --datatype mrs \
  --ds-initials HP \
  --compress-source

# Example 5: Enable defacing
echo "Example 5: DICOM import with defacing"
ln2t_tools import \
  --dataset 2024-Test_Dataset \
  --participant-label 001 \
  --datatype dicom \
  --deface

# Example 6: Custom virtual environment
echo "Example 6: Using custom virtual environment"
ln2t_tools import \
  --dataset 2024-Colorful_Bear-6f9c8e8bfe82 \
  --participant-label 001 \
  --import-env ~/venvs/my_custom_env

# Example 7: Import all datatypes for one participant
echo "Example 7: Complete import for single participant"
ln2t_tools import \
  --dataset 2024-Multi_Modal-xyz789 \
  --participant-label 042 \
  --ds-initials MM \
  --datatype all \
  --compress-source

# Example 8: Batch import (using loops)
echo "Example 8: Batch import with loop"
IDLIST="001 002 003 004 005"
for ID in $IDLIST; do
  echo "Importing subject $ID..."
  ln2t_tools import \
    --dataset 2024-Colorful_Bear-6f9c8e8bfe82 \
    --participant-label $ID \
    --ds-initials CB \
    --datatype dicom
done
