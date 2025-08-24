# fivetran_oms

Projet dbt pour construire un schéma en étoile à partir :
- Salesforce (MY_CUSTOMER_C)
- Postgres (ORDERS, PRODUCTS, TIME)

## Structure

- models/staging : nettoyage des tables sources
- models/marts : modèles finaux en étoile (dimensions et faits)
