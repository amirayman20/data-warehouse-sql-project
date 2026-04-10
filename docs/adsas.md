flowchart LR

    %% CRM Section
    subgraph CRM_System [CRM System]
        direction TB
        crm_sales[crm_sales_details<br/>- prd_key<br/>- cst_id<br/>*Sales Transactions*]
        crm_prd[crm_prd_info<br/>- prd_key<br/>*Product Master Data*]
        crm_cust[crm_cust_info<br/>- cst_id<br/>- cst_key<br/>*Customer Master Data*]
    end

    %% ERP Section
    subgraph ERP_System [ERP System]
        direction TB
        erp_cat[erp_px_cat_g1v2<br/>- id<br/>*Product Categories*]
        erp_cust[erp_cust_az12<br/>- cid<br/>*Customer Extra Info*]
        erp_loc[erp_loc_a101<br/>- cid<br/>*Customer Location*]
    end

    %% Relationships
    crm_prd -- prd_key --> crm_sales
    crm_cust -- cst_id --> crm_sales

    erp_cust -- cid --> erp_loc
    erp_cat -- id --> erp_cat
