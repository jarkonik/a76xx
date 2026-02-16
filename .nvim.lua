-- You must enable the exrc setting in neovim for this config file to be used.
local rust_analyzer = {
	cargo = {
		features = { "defmt" }
	},
}

-- Note the neovim name of the language server is rust_analyzer with an underscore.
vim.lsp.config("rust_analyzer", {
	settings = {
		["rust-analyzer"] = rust_analyzer
	},
})
