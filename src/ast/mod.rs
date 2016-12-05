use tokens::*;

#[derive(Debug)]
pub struct Statement;

#[derive(Debug)]
pub struct Ast
{
	pub statements: Vec<Statement>
}

impl Ast
{
	fn parse(tokens: &Vec<Token>) -> Self
	{
		Self { statements: Vec::new() }
	}
}
