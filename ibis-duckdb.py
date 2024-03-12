import ibis
import ibis.selectors as sel


"""
https://ibis-project.org/tutorials/getting_started
basic frontend tutorial using default duckdb backend
"""
def ibis_visualize():
    con = ibis.connect("duckdb://penguins.ddb")
    con.create_table("penguins", ibis.examples.penguins.fetch().to_pyarrow(), overwrite=True)

    penguins = con.table("penguins")

    print(penguins)  # just print the table metadata
    print(penguins.head())  # still just prints metadata and doesn't evaluate because ibis is lazily evaluated
    print(penguins.head().to_pandas())  # calling to_pandas forces the evaluation

    ibis.options.interactive = True  # this way queries are partially evaluated (max 10 rows)
    print(penguins.head())  # no more need to call to_pandas

    # now let's perform queries on the dataset
    # important: unlike pandas, tables are immutable so operations return new tables

    # SELECT
    selected = penguins.select("species", "island", penguins.year)  # mixing styles: string col name and dot name
    print(selected)

    # add columns
    added_col = penguins.mutate(bill_length_cm=penguins.bill_length_mm / 10)  # adding column computed from other column
    print(added_col)

    # SELECT better: match on column name/other
    selected_enhanced = (
        penguins.mutate(bill_length_cm=penguins.bill_length_mm / 10).select(
            ~sel.matches("bill_length_mm")
            # match every column except `bill_length_mm`
            # alternative: use sel.numeric() to exclude string columns
        ))
    print(selected_enhanced)

    # ORDER BY
    ordered = (penguins
               .order_by(penguins.flipper_length_mm.desc())
               .select("species", "island", "flipper_length_mm"))
    print(ordered)

    # aggregate
    penguins.flipper_length_mm.mean()
    penguins.aggregate([penguins.flipper_length_mm.mean(), penguins.bill_depth_mm.max()])

    # GROUP BY + aggregators
    penguins.group_by("species").aggregate()
    penguins.group_by(["species", "island"]).aggregate()  # get all unique pairings
    penguins.group_by(["species", "island"]).aggregate(  # mean and max over distinct groups
        [penguins.bill_length_mm.mean(), penguins.flipper_length_mm.max()]
    )

    # largest female penguin (by body mass) on each island in the year 2008
    (penguins
     .filter((penguins.sex == "female") & (penguins.year == 2008))
     .group_by(["island"])
     .aggregate(penguins.body_mass_g.max()))

    # largest male penguin (by body mass) on each island for each year of data collection
    (penguins
     .filter(penguins.sex == "male")
     .group_by(["island", "year"])
     .aggregate(penguins.body_mass_g.max().name("max_body_mass"))
     .order_by(["year", "max_body_mass"]))

    # count Adelie penguins with mass over 3500 by sex
    (penguins
     .filter([penguins.species == "Adelie", penguins.body_mass_g > 3500])
     .sex.value_counts()
     .dropna("sex")
     .order_by("sex"))


"""
https://ibis-project.org/tutorials/ibis-for-pandas-users
frontend tutorial for pandas users: describes similarities between the APIs
"""


if __name__ == '__main__':
    ibis_visualize()
