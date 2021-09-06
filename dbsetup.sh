while getopts "d:" flag
do
    case "${flag}" in
        d) database="${OPTARG}";;
    esac
done

createdb "${database:-shopify}" "Shopify customer, inventory, product and order database" --owner="$USER" --username="$USER"

psql -d "${database:-shopify}" -U "$USER" -f ./setup.sql

echo "Database setup completed"
