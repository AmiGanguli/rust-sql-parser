use ast;
use lexer;

use tokens::Token;
use ast::Ast;

pub struct ParseError
{
	message: &'static str,
	location: Option<Token>,
	expecting: Option<Token>,
}

pub enum ParseResult
{
	Error(ParseError),
	Success(Ast),
}

