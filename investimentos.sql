-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Tempo de geração: 06-Nov-2022 às 21:34
-- Versão do servidor: 10.6.10-MariaDB
-- versão do PHP: 8.0.24

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Banco de dados: `investimentos`
--

-- --------------------------------------------------------

--
-- Estrutura da tabela `fundos_cvm_cotas`
--

CREATE TABLE `fundos_cvm_cotas` (
  `id` int(11) NOT NULL,
  `CNPJ_FUNDO` varchar(18) NOT NULL,
  `DT_COMPTC` date NOT NULL,
  `VL_TOTAL` float NOT NULL,
  `VL_QUOTA` float NOT NULL,
  `CAPTC_DIA` float NOT NULL,
  `RESG_DIA` float NOT NULL,
  `NR_COTST` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- --------------------------------------------------------

--
-- Estrutura da tabela `fundos_cvm_descricao`
--

CREATE TABLE `fundos_cvm_descricao` (
  `id` int(11) NOT NULL,
  `CNPJ_FUNDO` varchar(18) NOT NULL,
  `DT_COMPTC` date NOT NULL,
  `DENOM_SOCIAL` varchar(150) NOT NULL,
  `NM_FANTASIA` varchar(150) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- --------------------------------------------------------

--
-- Estrutura da tabela `scrapy_fundos_cvm_cotas`
--

CREATE TABLE `scrapy_fundos_cvm_cotas` (
  `id` int(11) NOT NULL,
  `link` varchar(50) NOT NULL,
  `ultima_atualizacao` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- --------------------------------------------------------

--
-- Estrutura da tabela `scrapy_fundos_cvm_descricao`
--

CREATE TABLE `scrapy_fundos_cvm_descricao` (
  `id` int(11) NOT NULL,
  `link` varchar(50) NOT NULL,
  `ultima_atualizacao` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

--
-- Índices para tabelas despejadas
--

--
-- Índices para tabela `fundos_cvm_cotas`
--
ALTER TABLE `fundos_cvm_cotas`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `CNPJ_FUNDO_x_DT_COMPTC` (`CNPJ_FUNDO`,`DT_COMPTC`);

--
-- Índices para tabela `fundos_cvm_descricao`
--
ALTER TABLE `fundos_cvm_descricao`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `CNPJ_FUNDO_unique` (`CNPJ_FUNDO`);

--
-- Índices para tabela `scrapy_fundos_cvm_cotas`
--
ALTER TABLE `scrapy_fundos_cvm_cotas`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `link_unique` (`link`);

--
-- Índices para tabela `scrapy_fundos_cvm_descricao`
--
ALTER TABLE `scrapy_fundos_cvm_descricao`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `link_unique` (`link`);

--
-- AUTO_INCREMENT de tabelas despejadas
--

--
-- AUTO_INCREMENT de tabela `fundos_cvm_cotas`
--
ALTER TABLE `fundos_cvm_cotas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `fundos_cvm_descricao`
--
ALTER TABLE `fundos_cvm_descricao`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `scrapy_fundos_cvm_cotas`
--
ALTER TABLE `scrapy_fundos_cvm_cotas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `scrapy_fundos_cvm_descricao`
--
ALTER TABLE `scrapy_fundos_cvm_descricao`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
