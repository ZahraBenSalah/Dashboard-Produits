import pandas as pd

def test_discount_calculation():
    df = pd.DataFrame({'price':[100, 200], 'actual_price':[150, 250]})
    df['discount'] = df['actual_price'] - df['price']
    assert df['discount'].tolist() == [50, 50]

def test_merge_products_prices():
    products_df = pd.DataFrame({'product_id':[1,2]})
    prices_df = pd.DataFrame({'product_id':[1,2], 'price':[100,200]})
    merged = products_df.merge(prices_df, on='product_id', how='inner')
    assert merged['price'].tolist() == [100,200]

def test_empty_dataframe():
    df = pd.DataFrame(columns=['price','actual_price'])
    assert df.empty

def test_aggregation():
    df = pd.DataFrame({
        'category':['A','A','B'],
        'sub_category':['X','X','Y'],
        'price':[100,200,300],
        'discount':[10,20,30]
    })
    summary = df.groupby(['category','sub_category']).agg(
        avg_price=('price','mean'),
        avg_discount=('discount','mean')
    ).reset_index()
    assert summary.loc[summary['category']=='A','avg_price'].iloc[0] == 150

def test_price_conversion():
    df = pd.DataFrame({'price':['₹100','â‚¹200','300']})
    df['price'] = df['price'].str.replace("â‚¹","",regex=False).str.replace("₹","",regex=False)
    df['price'] = pd.to_numeric(df['price'],errors='coerce')
    assert df['price'].tolist() == [100,200,300]