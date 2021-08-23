while getopts "d:" flag
do
    case "${flag}" in
        d) database="${OPTARG:-shopify}";;
    esac
done
createdb "$database" "Shopify customer, inventory, product and order database" --owner="$USER" --username="$USER"

psql -d "$database" -U "$USER" -f ./setup.sql

echo "Database setup completed"
