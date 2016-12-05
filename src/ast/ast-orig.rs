// AST derived from LlambdaDB and https://sqlite.org/lang.html
//

#[derive(Debug)]
pub struct Ast
{
	pub statements: Vec<Statement>
}

#[derive(Debug)]
pub struct Statement
{
	pub explain: bool,
	pub query_plan: bool,
	pub body: Body,
}

#[derive(Debug)]
pub enum Body
{
	Alter(AlterStatement),
	Analyze(AnalyzeStatement),
	Attach(AttachStatement),
	Begin(BeginStatement),
	Commit(CommitStatement),
	Create(CreateStatement),
	Delete(DeleteStatement),
	Drop(DropStatement),
	Insert(InsertStatement),
	Pragma(PragmaStatement),
	Reindex(ReindexStatement),
	Release(ReleaseStatement),
	Rollback(RollbackStatement),
	Savepoint(SavepointStatement),
	Select(SelectStatement),
	Update(UpdateStatement),
	Vacuum(VacuumStatement),
}

// Stubs not yet implemented

#[derive(Debug)]
pub struct AlterStatement
{
}
#[derive(Debug)]
pub struct AnalyzeStatement
{
}
#[derive(Debug)]
pub struct AttachStatement
{
}
#[derive(Debug)]
pub struct BeginStatement
{
}
#[derive(Debug)]
pub struct CommitStatement
{
}
#[derive(Debug)]
pub struct CreateStatement
{
}
#[derive(Debug)]
pub struct DeleteStatement
{
}
#[derive(Debug)]
pub struct DropStatement
{
}
#[derive(Debug)]
pub struct InsertStatement
{
}
#[derive(Debug)]
pub struct PragmaStatement
{
}
#[derive(Debug)]
pub struct ReindexStatement
{
}
#[derive(Debug)]
pub struct ReleaseStatement
{
}
#[derive(Debug)]
pub struct RollbackStatement
{
}
#[derive(Debug)]
pub struct SavepointStatement
{
}
#[derive(Debug)]
pub struct SelectStatement
{
}
#[derive(Debug)]
pub struct UpdateStatement
{
}
#[derive(Debug)]
pub struct VacuumStatement
{
}


// Code from Llambda not yet implemented ==========================================

/*
#[derive(Debug, PartialEq)]
pub enum UnaryOp {
	Negate
}

#[derive(Debug, PartialEq)]
pub enum BinaryOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    BitAnd,
    BitOr,
    Concatenate,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Ident(String),
    IdentMember(String, String),
    StringLiteral(String),
    Number(String),
    Null,
    /// name(argument1, argument2, argument3...)
    FunctionCall { name: String, arguments: Vec<Expression> },
    /// name(*)
    FunctionCallAggregateAll { name: String },
    UnaryOp {
        expr: Box<Expression>,
        op: UnaryOp
    },
    /// lhs op rhs
    BinaryOp {
        lhs: Box<Expression>,
        rhs: Box<Expression>,
        op: BinaryOp
    },
    Subquery(Box<SelectStatement>)
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub database_name: Option<String>,
    pub table_name: String
}

#[derive(Debug, PartialEq)]
pub enum TableOrSubquery {
    Subquery {
        subquery: Box<SelectStatement>,
        alias: String
    },
    Table {
        table: Table,
        alias: Option<String>
    }
}

#[derive(Debug, PartialEq)]
pub enum SelectColumn {
    AllColumns,
    Expr {
        expr: Expression,
        alias: Option<String>
    }
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub result_columns: Vec<SelectColumn>,
    pub from: From,
    pub where_expr: Option<Expression>,
    pub group_by: Vec<Expression>,
    pub having: Option<Expression>,
    pub order_by: Vec<OrderingTerm>
}

#[derive(Debug, PartialEq)]
pub enum From {
    Cross(Vec<TableOrSubquery>),
    Join {
        table: TableOrSubquery,
        joins: Vec<Join>
    }
}

#[derive(Debug, PartialEq)]
pub enum JoinOperator {
    Left,
    Inner
}

#[derive(Debug, PartialEq)]
pub struct Join {
    pub operator: JoinOperator,
    pub table: TableOrSubquery,
    pub on: Expression
}

#[derive(Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending
}

#[derive(Debug, PartialEq)]
pub struct OrderingTerm {
    pub expr: Expression,
    pub order: Order
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table: Table,
    pub into_columns: Option<Vec<String>>,
    pub source: InsertSource
}

#[derive(Debug)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),
    Select(Box<SelectStatement>)
}

#[derive(Debug)]
pub struct CreateTableColumnConstraint {
    pub name: Option<String>,
    pub constraint: CreateTableColumnConstraintType
}

#[derive(Debug, PartialEq)]
pub enum CreateTableColumnConstraintType {
    PrimaryKey,
    Unique,
    Nullable,
    ForeignKey {
        table: Table,
        columns: Option<Vec<String>>
    }
}

#[derive(Debug)]
pub struct CreateTableColumn {
    pub column_name: String,
    pub type_name: String,
    pub type_size: Option<String>,
    /// * None if no array
    /// * Some(None) if dynamic array: type[]
    /// * Some(Some(_)) if fixed array: type[SIZE]
    pub type_array_size: Option<Option<String>>,
    pub constraints: Vec<CreateTableColumnConstraint>
}

#[derive(Debug)]
pub struct CreateTableStatement {
    pub table: Table,
    pub columns: Vec<CreateTableColumn>
}

#[derive(Debug)]
pub enum CreateStatement {
    Table(CreateTableStatement)
}
*/



