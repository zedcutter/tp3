CREATE TABLE {{owner}}.{{tablename}} (

	{{{ {{tablename}}_colinfo::
		{{!1::,}} {{.colname}}	{{.datatype}}	{{.nullable}}
	}}}
	
		, CONSTRAINT {{tablename}}_PK PRIMARY KEY (
		
		{{{ {{tablename}}_pkcols::
		    {{!1::,}} {{.colname}}
		}}}
		
		)
);

{{{{{tablename}}_colinfo::
COMMENT ON COLUMN {{owner}}.{{tablename}}.{{.colname}} IS '{{.col_comment}}';
}}}