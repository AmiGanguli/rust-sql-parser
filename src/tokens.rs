#[derive(Clone, Debug, PartialEq)]
pub struct Token
{
	pub token_type: TokenType,
	pub line: usize,
	pub column: usize,
	pub value: String,
}

impl Token
{
	pub fn new(tt: TokenType, line: usize, column: usize, value: String) -> Self
	{
		Self {
			token_type: tt,
			line: line,
			column: column,
			value: value
		}
	}
}


#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TokenType
{
	// Keywords
	Abort,
	Action,
	Add,
	After,
	All,
	Alter,
	Analyze,
	And,
	As,
	Asc,
	Attach,
	Autoincr,
	Before,
	Begin,
	Between,
	By,
	Cascade,
	Case,
	Cast,
	Check,
	Collate,
	Column,
	Commit,
	Conflict,
	Constraint,
	Create,
	CTime,
	Database,
	Default,
	Deferrable,
	Deferred,
	Delete,
	Desc,
	Detach,
	Distinct,
	Drop,
	Each,
	Else,
	End,
	Escape,
	Except,
	Exclusive,
	Exists,
	Explain,
	Fail,
	For,
	Foreign,
	From,
	Group,
	Having,
	If,
	Ignore,
	Immediate,
	In,
	Index,
	Indexed,
	Initially,
	Inner,
	Insert,
	Instead,
	Intersect,
	Into,
	Is,
	IsNull,
	Join,
	Key,
	Left,
	Like,
	Limit,
	Match,
	No,
	Not,
	NotNull,
	Null,
	Of,
	Offset,
	On,
	Or,
	Order,
	Plan,
	Pragma,
	Primary,
	Query,
	Raise,
	Recursive,
	References,
	Reindex,
	Release,
	Rename,
	Replace,
	Restrict,
	Rollback,
	Row,
	Savepoint,
	Select,
	Set,
	Table,
	Temp,
	Then,
	To,
	Transaction,
	Trigger,
	Union,
	Update,
	Using,
	Vacuum,
	Values,
	View,
	Virtual,
	With,
	Without,
	When,
	Where,

	// Non-letter tokens
	Equal,
	NotEqual,
	LessThan,
	LessThanOrEqual,
	GreaterThan,
	GreaterThanOrEqual,

	Plus, Minus,

	LeftParen, RightParen,
	LeftBracket, RightBracket,
	Dot, Comma, Semicolon,

	Ampersand, Pipe, ForwardSlash,

	/// ||, the concatenate operator
	DoublePipe,

	/// *, the wildcard for SELECT
	Asterisk,

	/// ?, the prepared statement placeholder
	PreparedStatementPlaceholder,
	
	// Tokens with values
	Ident,
	Number,
	StringLiteral,
}
