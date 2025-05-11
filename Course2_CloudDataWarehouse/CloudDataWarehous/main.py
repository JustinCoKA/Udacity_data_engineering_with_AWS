import sys
import os
from dotenv import load_dotenv
load_dotenv()

# Import modules
from infrastructure.aws_setup import (
    create_iam_role,
    create_redshift_cluster,
    check_cluster_status,
    delete_redshift_cluster
)

from sql import create_tables
from etl import etl


def print_menu():
    print("\n🎵 Sparkify Data Warehouse - Main Menu")
    print("1. Create IAM Role")
    print("2. Create Redshift Cluster")
    print("3. Check Cluster Status")
    print("4. Create Tables (Drop & Create)")
    print("5. Run ETL Process (S3 → Staging → Star Schema)")
    print("6. Run Full Pipeline (1 → 5)")
    print("7. Delete Redshift Cluster")
    print("8. Show current AWS identity")
    print("0. Exit")


def run_pipeline():
    print("\n🚀 Running Full Pipeline")
    create_iam_role()
    create_redshift_cluster()
    check_cluster_status()
    create_tables.main()
    etl.main()
    print("✅ Full pipeline execution complete")

def print_aws_identity():
    import boto3
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()

    print("\n🔐 Current AWS Identity")
    print(f"👤 Account ID: {identity['Account']}")
    print(f"🧑 User ID: {identity['UserId']}")
    print(f"🪪 ARN: {identity['Arn']}")

def main():
    while True:
        print_menu()
        choice = input("Select an option (0–8): ")

        if choice == "1":
            create_iam_role()
        elif choice == "2":
            create_redshift_cluster()
        elif choice == "3":
            check_cluster_status()
        elif choice == "4":
            create_tables.main()
        elif choice == "5":
            etl.main()
        elif choice == "6":
            run_pipeline()
        elif choice == "7":
            delete_redshift_cluster()
        elif choice == "8":
            print_aws_identity()
        elif choice == "0":
            print("👋 Exiting. Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid selection. Please try again.")


if __name__ == "__main__":
    main()
